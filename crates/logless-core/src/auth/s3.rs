//! The read side of a principal: what part of the store it may read over S3 ([`ReadScope`]), and
//! temporary S3 credentials for a person or a service that holds a broker token ([`Sessions`]).
//!
//! A temporary credential is **stateless**. The session token carries the scope and the expiry and is
//! authenticated with an HMAC under a key this instance generates once and keeps in its data dir; the
//! access key id and the secret are derived from the same token under the same key. The S3 server needs
//! the request and that key to decide, and nothing else: there is no table of sessions, so a restart
//! forgets nothing and there is nothing to revoke. A credential ends when its expiry passes.

use std::path::Path;
use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::{b64url, Indexes, Kind, Principal, Refusal, Source, EVERY_TENANT};

/// The HMAC key of the temporary S3 credentials, in the data dir, readable by its owner only.
pub const SESSION_KEY_FILE: &str = "s3-session.key";
/// What a temporary access key id starts with, so it is told from a configured reader at a glance.
pub const TEMP_KEY_PREFIX: &str = "LLTMP";
/// The header (lowercase) a temporary credential's session token travels in.
pub const SESSION_TOKEN_HEADER: &str = "x-amz-security-token";
const VERSION: &str = "v1";
/// A session token is short; a longer value is refused unread.
const MAX_SESSION_TOKEN_LEN: usize = 4 * 1024;

type HmacSha256 = Hmac<Sha256>;

/// The key prefixes of the store a principal may read. One index of one tenant is one prefix
/// (`tenant=<t>/index=<i>/`), so a scope is a short list of them and a check is a prefix comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadScope {
    /// `""` is the whole store.
    roots: Vec<String>,
}

impl ReadScope {
    pub fn whole() -> Self {
        Self { roots: vec![String::new()] }
    }

    /// The scope of `principal`: its tenant, and within it the indexes on its list. A tenant or index
    /// name that could not be a directory name gives no prefix at all.
    pub fn of(principal: &Principal) -> Self {
        if principal.tenant == EVERY_TENANT {
            return Self::whole();
        }
        if !crate::partition::valid_ident(&principal.tenant) {
            return Self { roots: Vec::new() };
        }
        let tenant = format!("tenant={}/", principal.tenant);
        let roots = match &principal.indexes {
            Indexes::All => vec![tenant],
            Indexes::Only(list) => list.iter().filter(|i| crate::partition::valid_ident(i)).map(|i| format!("{tenant}index={i}/")).collect(),
        };
        Self { roots }
    }

    pub fn is_whole(&self) -> bool {
        self.roots.iter().any(String::is_empty)
    }

    /// Whether the object `key` may be read or listed.
    pub fn allows(&self, key: &str) -> bool {
        self.roots.iter().any(|root| key.starts_with(root.as_str()))
    }

    /// Whether a listing may walk into the directory `dir_key` (a key prefix ending in `/`): it is inside
    /// the scope, or on the way down to it.
    pub fn reaches(&self, dir_key: &str) -> bool {
        self.roots.iter().any(|root| dir_key.starts_with(root.as_str()) || root.starts_with(dir_key))
    }

    pub(super) fn to_json(&self) -> serde_json::Value {
        serde_json::json!(self.roots)
    }
}

/// Temporary S3 credentials, as `POST /v1/auth/s3-credentials` returns them.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expires_unix_secs: u64,
}

// Hand-written so a logged value never carries the secret or the token.
impl std::fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Credentials").field("access_key_id", &self.access_key_id).field("expires_unix_secs", &self.expires_unix_secs).finish_non_exhaustive()
    }
}

/// Mints and checks temporary S3 credentials under one key.
pub struct Sessions {
    key: [u8; 32],
    /// False when the key could not be kept on disk: credentials then end with the process.
    persisted: bool,
}

fn hmac(key: &[u8], label: &str, data: &str) -> [u8; 32] {
    // HMAC accepts any key length, so new_from_slice cannot fail.
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key length is unrestricted");
    mac.update(label.as_bytes());
    mac.update(b"\n");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn b64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk.iter().fold(0u32, |acc, b| (acc << 8) | u32::from(*b)) << (8 * (3 - chunk.len()));
        for i in 0..=chunk.len() {
            out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}

fn random_key() -> std::io::Result<[u8; 32]> {
    let mut key = [0u8; 32];
    getrandom::fill(&mut key).map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(key)
}

impl Sessions {
    /// The key in `data_dir`, made on first use. A key that cannot be read or written does not stop the
    /// instance: it runs with one held in memory, and says so.
    pub fn open(data_dir: &Path) -> Self {
        match Self::load_or_create(&data_dir.join(SESSION_KEY_FILE)) {
            Ok(key) => Self { key, persisted: true },
            Err(e) => {
                tracing::warn!(error = %e, file = SESSION_KEY_FILE, "the key of the temporary S3 credentials could not be kept in the data dir; credentials issued now end at the next restart");
                Self::in_memory()
            }
        }
    }

    /// A key that lives as long as this value: a process without a data dir, or a test.
    pub fn in_memory() -> Self {
        // Without a source of randomness there is no key to hand out credentials under; the process
        // could not have generated its node id either.
        Self { key: random_key().expect("the operating system's random source"), persisted: false }
    }

    fn load_or_create(path: &Path) -> std::io::Result<[u8; 32]> {
        match std::fs::read_to_string(path) {
            Ok(text) => match super::unhex32(&text) {
                Some(key) => return Ok(key),
                None => tracing::warn!(file = %path.display(), "the key of the temporary S3 credentials is not 64 hex digits; a new one is made and credentials issued under the old one stop working"),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let key = random_key()?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("key.tmp");
        let _ = std::fs::remove_file(&tmp);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        {
            use std::io::Write;
            let mut file = options.open(&tmp)?;
            file.write_all(hex(&key).as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(key)
    }

    pub fn persisted(&self) -> bool {
        self.persisted
    }

    fn derive(&self, payload: &str) -> (String, String, String) {
        let access_key_id = format!("{TEMP_KEY_PREFIX}{}", hex(&hmac(&self.key, "logless-s3-access-key-v1", payload)[..8]).to_ascii_uppercase());
        let secret = hex(&hmac(&self.key, "logless-s3-secret-v1", payload));
        let mac = b64(&hmac(&self.key, "logless-s3-session-v1", payload));
        (access_key_id, secret, mac)
    }

    /// Credentials for `principal`'s tenant and indexes, good until `expires_unix_secs`.
    pub fn issue(&self, principal: &Principal, expires_unix_secs: u64) -> S3Credentials {
        let payload = b64(
            serde_json::json!({
                "sub": principal.id,
                "kind": principal.kind.as_str(),
                "tenant": principal.tenant,
                "indexes": principal.indexes.to_json(),
                "exp": expires_unix_secs,
            })
            .to_string()
            .as_bytes(),
        );
        let (access_key_id, secret_access_key, mac) = self.derive(&payload);
        S3Credentials { access_key_id, secret_access_key, session_token: format!("{VERSION}.{payload}.{mac}"), expires_unix_secs }
    }

    /// Who a session token is and the key pair its requests are signed with. The MAC is judged before
    /// anything of the payload is read; a token past its expiry is refused whatever else is right.
    pub fn verify(&self, token: &str, now_unix_secs: u64) -> Result<(Arc<Principal>, crate::s3_sigv4::Credentials), Refusal> {
        if token.len() > MAX_SESSION_TOKEN_LEN {
            return Err(Refusal::Malformed);
        }
        let mut parts = token.split('.');
        let (Some(VERSION), Some(payload), Some(mac), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
            return Err(Refusal::Malformed);
        };
        let presented = b64url(mac).ok_or(Refusal::Malformed)?;
        let mut expected = HmacSha256::new_from_slice(&self.key).expect("hmac key length is unrestricted");
        expected.update(b"logless-s3-session-v1\n");
        expected.update(payload.as_bytes());
        expected.verify_slice(&presented).map_err(|_| Refusal::Signature)?;

        let claims: serde_json::Value = b64url(payload).and_then(|bytes| serde_json::from_slice(&bytes).ok()).ok_or(Refusal::Claims)?;
        let text = |name: &str| claims.get(name).and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        let (Some(sub), Some(kind), Some(tenant), Some(exp)) = (text("sub"), text("kind").and_then(Kind::parse), text("tenant"), claims.get("exp").and_then(|e| e.as_u64())) else {
            return Err(Refusal::Claims);
        };
        let indexes: Vec<String> = match claims.get("indexes") {
            Some(serde_json::Value::Array(items)) => items.iter().map(|i| i.as_str().map(str::to_string).ok_or(Refusal::Claims)).collect::<Result<_, _>>()?,
            _ => return Err(Refusal::Claims),
        };
        if exp <= now_unix_secs {
            return Err(Refusal::Expired);
        }
        let (access_key, secret_key, _) = self.derive(payload);
        let principal = Principal {
            id: sub.to_string(),
            kind,
            tenant: tenant.to_string(),
            indexes: Indexes::from_list(&indexes),
            scopes: Vec::new(),
            roles: Vec::new(),
            source: Source::Session,
            expires_unix_secs: Some(exp),
            only_at: Some(super::Listener::S3),
        };
        Ok((Arc::new(principal), crate::s3_sigv4::Credentials { access_key, secret_key }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_800_000_000;

    fn who(tenant: &str, indexes: Indexes) -> Principal {
        Principal { id: "alice".into(), kind: Kind::User, tenant: tenant.into(), indexes, scopes: Vec::new(), roles: vec!["viewer".into()], source: Source::Token, expires_unix_secs: Some(NOW + 3600), only_at: None }
    }

    fn only(list: &[&str]) -> Indexes {
        Indexes::Only(list.iter().map(|i| i.to_string()).collect())
    }

    #[test]
    fn a_scope_is_the_tenant_and_its_listed_indexes_and_nothing_beside_them() {
        let table: [(&str, Indexes, &str, bool, bool); 14] = [
            // tenant, indexes, key or directory, may read the key, may walk the directory
            ("acme", Indexes::All, "tenant=acme/index=default/hour=2026-01-01T00/level=info/node=n/a.parquet", true, true),
            ("acme", Indexes::All, "tenant=acme/", true, true),
            ("acme", Indexes::All, "tenant=acme2/index=default/a.parquet", false, false),
            ("acme", Indexes::All, "tenant=default/index=default/a.parquet", false, false),
            ("acme", Indexes::All, "tenant=ACME/index=default/a.parquet", false, false),
            ("acme", Indexes::All, "a.parquet", false, false),
            ("acme", only(&["payments"]), "tenant=acme/index=payments/_meta/templates/writer=n.pq", true, true),
            ("acme", only(&["payments"]), "tenant=acme/index=default/_meta/templates/writer=n.pq", false, false),
            ("acme", only(&["payments"]), "tenant=acme/index=payments2/a.parquet", false, false),
            // On the way down to the scope: walked, never read.
            ("acme", only(&["payments"]), "tenant=acme/", false, true),
            ("acme", only(&["payments", "default"]), "tenant=acme/index=default/", true, true),
            ("acme", only(&[]), "tenant=acme/index=default/a.parquet", false, false),
            ("acme", only(&["../x"]), "tenant=acme/index=../x/a.parquet", false, false),
            ("*", Indexes::All, "tenant=anything/index=x/a.parquet", true, true),
        ];
        for (tenant, indexes, key, read, walk) in table {
            let scope = ReadScope::of(&who(tenant, indexes.clone()));
            assert_eq!((read, walk), (scope.allows(key), scope.reaches(key)), "{tenant} {indexes:?} {key}");
        }
        assert!(ReadScope::whole().is_whole() && ReadScope::of(&who("*", Indexes::All)).is_whole());
        assert!(!ReadScope::of(&who("acme", Indexes::All)).is_whole());
        assert!(!ReadScope::of(&who("../etc", Indexes::All)).allows("tenant=../etc/x.parquet"), "a tenant that cannot be a directory reads nothing");
    }

    #[test]
    fn a_credential_round_trips_and_carries_the_scope_it_was_issued_with() {
        let sessions = Sessions::in_memory();
        let issued = sessions.issue(&who("acme", only(&["payments"])), NOW + 600);
        assert!(issued.access_key_id.starts_with(TEMP_KEY_PREFIX) && issued.access_key_id.len() == TEMP_KEY_PREFIX.len() + 16);
        assert_eq!(64, issued.secret_access_key.len());
        let (principal, creds) = sessions.verify(&issued.session_token, NOW).unwrap();
        assert_eq!((principal.id.as_str(), principal.kind, principal.tenant.as_str(), principal.source), ("alice", Kind::User, "acme", Source::Session));
        assert_eq!(principal.indexes, only(&["payments"]));
        assert_eq!(principal.expires_unix_secs, Some(NOW + 600));
        assert_eq!((creds.access_key, creds.secret_key), (issued.access_key_id.clone(), issued.secret_access_key.clone()));
        // Every index stays every index.
        let all = sessions.issue(&who("acme", Indexes::All), NOW + 600);
        assert_eq!(sessions.verify(&all.session_token, NOW).unwrap().0.indexes, Indexes::All);
        assert_ne!(all.secret_access_key, issued.secret_access_key, "another scope is another secret");
        // Nothing of the secret or the token is in a logged value.
        let shown = format!("{issued:?}");
        assert!(!shown.contains(&issued.secret_access_key) && !shown.contains(&issued.session_token), "{shown}");
    }

    #[test]
    fn an_expired_or_changed_token_is_refused_and_the_secret_follows_the_token() {
        let sessions = Sessions::in_memory();
        let issued = sessions.issue(&who("acme", only(&["payments"])), NOW + 600);
        let verdict = |token: &str, now: u64| sessions.verify(token, now).map(|_| ()).map_err(Refusal::as_str);
        assert_eq!(Ok(()), verdict(&issued.session_token, NOW + 599));
        assert_eq!(Err("expired"), verdict(&issued.session_token, NOW + 600), "no allowance: the expiry is this instance's own clock");
        assert_eq!(Err("expired"), verdict(&issued.session_token, NOW + 86_400));

        // The scope or the expiry rewritten under the old MAC.
        let parts: Vec<&str> = issued.session_token.split('.').collect();
        let rewrite = |change: &dyn Fn(&mut serde_json::Value)| {
            let mut claims: serde_json::Value = serde_json::from_slice(&b64url(parts[1]).unwrap()).unwrap();
            change(&mut claims);
            format!("{}.{}.{}", parts[0], b64(claims.to_string().as_bytes()), parts[2])
        };
        assert_eq!(Err("signature"), verdict(&rewrite(&|c| c["tenant"] = "default".into()), NOW), "another tenant");
        assert_eq!(Err("signature"), verdict(&rewrite(&|c| c["indexes"] = serde_json::json!(["*"])), NOW), "more indexes");
        assert_eq!(Err("signature"), verdict(&rewrite(&|c| c["exp"] = (NOW + 86_400 * 365).into()), NOW + 700), "a later expiry");
        // The payload of one credential under the MAC of another.
        let other = sessions.issue(&who("default", Indexes::All), NOW + 600);
        let other_parts: Vec<&str> = other.session_token.split('.').collect();
        assert_eq!(Err("signature"), verdict(&format!("v1.{}.{}", other_parts[1], parts[2]), NOW));
        // A key this instance does not hold.
        assert_eq!(Err("signature"), Sessions::in_memory().verify(&issued.session_token, NOW).map(|_| ()).map_err(Refusal::as_str));
        for junk in ["", "v1", "v1..", "v2.a.b", &format!("{}.x", issued.session_token), "v1.!!!.???", &"a".repeat(MAX_SESSION_TOKEN_LEN + 1)] {
            assert!(matches!(verdict(junk, NOW), Err("malformed" | "signature")), "{:.40}", junk);
        }
    }

    #[test]
    fn the_key_is_made_once_kept_private_and_outlives_a_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let first = Sessions::open(tmp.path());
        assert!(first.persisted());
        let path = tmp.path().join(SESSION_KEY_FILE);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(0o600, std::fs::metadata(&path).unwrap().permissions().mode() & 0o777);
        }
        let issued = first.issue(&who("acme", Indexes::All), NOW + 600);
        drop(first);
        // A restart: the same file, the same key, the credential still good.
        let second = Sessions::open(tmp.path());
        let (principal, creds) = second.verify(&issued.session_token, NOW).unwrap();
        assert_eq!((principal.tenant.as_str(), creds.secret_key), ("acme", issued.secret_access_key.clone()));
        // A key file that is not a key is replaced, and what was issued under the old one stops.
        std::fs::write(&path, "not a key").unwrap();
        let third = Sessions::open(tmp.path());
        assert!(third.persisted());
        assert_eq!(Err(Refusal::Signature), third.verify(&issued.session_token, NOW).map(|_| ()));
        assert_eq!(64, std::fs::read_to_string(&path).unwrap().len());
        // A data dir that cannot hold the key does not stop anything: the key is in memory.
        let file_not_dir = tmp.path().join("plain-file");
        std::fs::write(&file_not_dir, "x").unwrap();
        let fallback = Sessions::open(&file_not_dir);
        assert!(!fallback.persisted());
        let again = fallback.issue(&who("acme", Indexes::All), NOW + 600);
        assert!(fallback.verify(&again.session_token, NOW).is_ok());
    }
}
