//! DSNs, `X-Sentry-Auth`, and the two ways to authenticate upstream.
//!
//! A Sentry DSN is `<scheme>://<public key>@<host>[:port]/<project id>`. The
//! public key is not a secret in the sense a password is — it is embedded in
//! browser bundles — but it *is* the credential that decides which project an
//! event lands in, and that makes the choice of what to send upstream a routing
//! decision as much as an authentication one.

/// How the proxy authenticates to upstream Sentry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamAuth {
    /// Forward the key and project the client presented, changing only the
    /// host. Every application keeps its own DSN, so events land in the same
    /// project they would have without the proxy and Sentry's per-project
    /// quotas, alerts and ownership rules keep working untouched.
    ///
    /// Requires `upstream_host`: the client's DSN names *us*, so the host it
    /// presents is not where the event should go.
    Relay,
    /// Ignore what the client presented and use one configured DSN. Every event
    /// lands in one project regardless of origin — simpler to set up, and it
    /// discards per-project attribution, so the original key is preserved as a
    /// tag rather than silently lost.
    Resign,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dsn {
    pub scheme: String,
    pub key: String,
    pub host: String,
    pub project: String,
}

impl Dsn {
    pub fn parse(dsn: &str) -> Option<Self> {
        let (scheme, rest) = dsn.split_once("://")?;
        let (credentials, host_and_path) = rest.rsplit_once('@')?;
        // Legacy DSNs carry `public:secret@`; the secret has been ignored by
        // Sentry for years, and keeping it would mean logging a credential.
        let key = credentials.split(':').next()?;
        let (host, project) = host_and_path.trim_end_matches('/').rsplit_once('/')?;
        if key.is_empty() || host.is_empty() || project.is_empty() {
            return None;
        }
        Some(Self {
            scheme: scheme.to_string(),
            key: key.to_string(),
            host: host.to_string(),
            project: project.to_string(),
        })
    }

    pub fn origin(&self) -> String {
        format!("{}://{}", self.scheme, self.host)
    }

    pub fn envelope_url(&self) -> String {
        format!("{}/api/{}/envelope/", self.origin(), self.project)
    }
}

/// The credential a client presented, however it presented it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientAuth {
    pub key: String,
    pub client: Option<String>,
    pub version: Option<String>,
}

/// Parses `X-Sentry-Auth: Sentry sentry_key=…, sentry_version=7, sentry_client=…`.
///
/// Case-insensitive on the scheme, tolerant of missing spaces and of the
/// key/value pairs arriving in any order — SDKs differ, and rejecting a valid
/// client over whitespace would look like an outage.
pub fn parse_auth_header(header: &str) -> Option<ClientAuth> {
    let rest = header
        .trim()
        .strip_prefix("Sentry ")
        .or_else(|| header.trim().strip_prefix("sentry "))
        .or_else(|| {
            // Some SDKs omit the space after the scheme.
            let lower = header.trim().to_ascii_lowercase();
            lower.starts_with("sentry").then(|| header.trim()[6..].trim_start())
        })?;

    let mut key = None;
    let mut client = None;
    let mut version = None;
    for pair in rest.split(',') {
        let Some((name, value)) = pair.split_once('=') else { continue };
        let value = value.trim().trim_matches('"').to_string();
        match name.trim().to_ascii_lowercase().as_str() {
            "sentry_key" => key = Some(value),
            "sentry_client" => client = Some(value),
            "sentry_version" => version = Some(value),
            _ => {}
        }
    }
    Some(ClientAuth { key: key?, client, version })
}

/// Builds the `X-Sentry-Auth` value sent upstream.
pub fn auth_header(key: &str, client: &str) -> String {
    format!("Sentry sentry_version=7, sentry_client={client}, sentry_key={key}")
}

/// Where a proxied envelope should go, and with what credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamTarget {
    pub url: String,
    pub key: String,
    pub project: String,
}

/// Resolves the upstream target for a request.
///
/// `relay` keeps the client's key and project and swaps only the host;
/// `resign` uses the configured DSN wholesale. A per-project override wins over
/// either, which is what makes a single proxy usable in front of several
/// upstream projects.
pub fn resolve(
    mode: UpstreamAuth,
    client_key: &str,
    client_project: &str,
    upstream_host: Option<&Dsn>,
    configured: Option<&Dsn>,
    override_dsn: Option<&Dsn>,
) -> Option<UpstreamTarget> {
    if let Some(dsn) = override_dsn {
        return Some(UpstreamTarget {
            url: dsn.envelope_url(),
            key: dsn.key.clone(),
            project: dsn.project.clone(),
        });
    }
    match mode {
        UpstreamAuth::Relay => {
            let host = upstream_host.or(configured)?;
            Some(UpstreamTarget {
                url: format!("{}/api/{}/envelope/", host.origin(), client_project),
                key: client_key.to_string(),
                project: client_project.to_string(),
            })
        }
        UpstreamAuth::Resign => {
            let dsn = configured?;
            Some(UpstreamTarget {
                url: dsn.envelope_url(),
                key: dsn.key.clone(),
                project: dsn.project.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dsn(s: &str) -> Dsn {
        Dsn::parse(s).unwrap()
    }

    #[test]
    fn parses_a_standard_dsn() {
        let d = dsn("https://abc123@o1.ingest.sentry.io/4505");
        assert_eq!(d.key, "abc123");
        assert_eq!(d.host, "o1.ingest.sentry.io");
        assert_eq!(d.project, "4505");
        assert_eq!(d.envelope_url(), "https://o1.ingest.sentry.io/api/4505/envelope/");
    }

    #[test]
    fn a_port_and_a_path_prefix_survive() {
        let d = dsn("http://k@127.0.0.1:9000/2");
        assert_eq!(d.host, "127.0.0.1:9000");
        assert_eq!(d.envelope_url(), "http://127.0.0.1:9000/api/2/envelope/");
    }

    #[test]
    fn the_legacy_secret_half_is_discarded() {
        // Sentry ignored the secret years ago; carrying it would mean logging
        // a credential for no benefit.
        let d = dsn("https://public:secret@sentry.io/1");
        assert_eq!(d.key, "public");
    }

    #[test]
    fn malformed_dsns_are_rejected() {
        for bad in [
            "not a dsn",
            "https://sentry.io/1",         // no key
            "https://k@sentry.io",         // no project
            "https://@sentry.io/1",        // empty key
            "https://k@/1",                // empty host
        ] {
            assert!(Dsn::parse(bad).is_none(), "{bad} should not parse");
        }
    }

    #[test]
    fn parses_the_auth_header_in_any_order() {
        let auth = parse_auth_header(
            "Sentry sentry_version=7, sentry_client=sentry.python/2.0, sentry_key=abc",
        )
        .unwrap();
        assert_eq!(auth.key, "abc");
        assert_eq!(auth.client.as_deref(), Some("sentry.python/2.0"));
        assert_eq!(auth.version.as_deref(), Some("7"));

        let reordered = parse_auth_header("Sentry sentry_key=abc,sentry_version=7").unwrap();
        assert_eq!(reordered.key, "abc");
    }

    #[test]
    fn auth_header_parsing_tolerates_sdk_quirks() {
        assert_eq!(parse_auth_header("sentry sentry_key=k").unwrap().key, "k");
        assert_eq!(parse_auth_header("Sentry  sentry_key = k ").unwrap().key, "k");
        assert_eq!(parse_auth_header("Sentry sentry_key=\"k\"").unwrap().key, "k");
        assert!(parse_auth_header("Bearer abc").is_none());
        assert!(parse_auth_header("Sentry sentry_version=7").is_none(), "no key is no auth");
    }

    #[test]
    fn relay_keeps_the_clients_key_and_project_and_changes_only_the_host() {
        // The point of relay mode: events land in the project they would have
        // without the proxy, so quotas, alerts and ownership keep working.
        let upstream = dsn("https://ignored@o1.ingest.sentry.io/9999");
        let target = resolve(
            UpstreamAuth::Relay,
            "app_key",
            "1234",
            Some(&upstream),
            None,
            None,
        )
        .unwrap();
        assert_eq!(target.key, "app_key");
        assert_eq!(target.project, "1234");
        assert_eq!(target.url, "https://o1.ingest.sentry.io/api/1234/envelope/");
    }

    #[test]
    fn resign_uses_the_configured_dsn_wholesale() {
        let configured = dsn("https://ours@o1.ingest.sentry.io/42");
        let target =
            resolve(UpstreamAuth::Resign, "app_key", "1234", None, Some(&configured), None).unwrap();
        assert_eq!(target.key, "ours");
        assert_eq!(target.project, "42");
        assert_eq!(target.url, "https://o1.ingest.sentry.io/api/42/envelope/");
    }

    #[test]
    fn a_per_project_override_wins_over_either_mode() {
        let configured = dsn("https://ours@sentry.io/42");
        let override_dsn = dsn("https://special@other.sentry.io/7");
        for mode in [UpstreamAuth::Relay, UpstreamAuth::Resign] {
            let target = resolve(mode, "app", "1234", None, Some(&configured), Some(&override_dsn))
                .unwrap();
            assert_eq!(target.project, "7", "{mode:?}");
            assert_eq!(target.key, "special");
        }
    }

    #[test]
    fn relay_without_an_upstream_host_falls_back_to_the_configured_dsns_host() {
        let configured = dsn("https://ours@o1.ingest.sentry.io/42");
        let target =
            resolve(UpstreamAuth::Relay, "app_key", "1234", None, Some(&configured), None).unwrap();
        assert_eq!(target.key, "app_key", "relay still keeps the client key");
        assert_eq!(target.url, "https://o1.ingest.sentry.io/api/1234/envelope/");
    }

    #[test]
    fn resign_without_a_configured_dsn_resolves_to_nothing() {
        // Better than inventing a destination: the caller reports a config
        // error instead of sending events somewhere arbitrary.
        assert!(resolve(UpstreamAuth::Resign, "k", "1", None, None, None).is_none());
        assert!(resolve(UpstreamAuth::Relay, "k", "1", None, None, None).is_none());
    }
}
