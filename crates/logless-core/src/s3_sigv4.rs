//! SigV4 header-auth verification for the read-only S3 endpoint, and query-string presigning for
//! Parquet part upload (`parts`).
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const MAX_SKEW_SECS: u64 = 900;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    Missing,
    Malformed,
    UnknownKey,
    Expired,
    BadSignature,
}

struct Parsed<'a> {
    access_key: &'a str,
    date: &'a str,
    region: &'a str,
    signed: Vec<String>,
    signature: Vec<u8>,
}

/// `headers` are as received (any case). Header auth only.
pub fn verify(
    creds: &Credentials,
    method: &str,
    raw_path: &str,
    raw_query: &str,
    headers: &[(String, String)],
    now_unix_secs: u64,
) -> Result<(), AuthError> {
    let auth = header(headers, "authorization").ok_or(AuthError::Missing)?;
    let p = parse_authorization(auth)?;

    // Reported only after every other check and the signature itself, so a
    // wrong access key fails the same way, at the same point, as a wrong secret
    // and cannot be used to confirm a valid key id.
    let key_known = ct_eq(p.access_key.as_bytes(), creds.access_key.as_bytes());

    let (amz_date, request_secs) = request_time(headers)?;
    if amz_date.get(..8) != Some(p.date) {
        return Err(AuthError::Malformed);
    }
    if now_unix_secs.abs_diff(request_secs) > MAX_SKEW_SECS {
        return Err(AuthError::Expired);
    }

    if !p.signed.iter().any(|h| h == "host")
        || !(p.signed.iter().any(|h| h == "x-amz-date") || p.signed.iter().any(|h| h == "date"))
    {
        return Err(AuthError::Malformed);
    }
    let payload_hash = header(headers, "x-amz-content-sha256").ok_or(AuthError::Malformed)?;

    let canonical = canonical_request(
        method,
        raw_path,
        raw_query,
        headers,
        &p.signed,
        payload_hash.trim(),
    )?;
    let expected = signature_for(&creds.secret_key, &amz_date, p.date, p.region, &canonical);

    let mut mac = HmacSha256::new_from_slice(&expected.key).map_err(|_| AuthError::Malformed)?;
    mac.update(expected.string_to_sign.as_bytes());
    let signed_ok = mac.verify_slice(&p.signature).is_ok();
    match (key_known, signed_ok) {
        (true, true) => Ok(()),
        (true, false) => Err(AuthError::BadSignature),
        (false, _) => Err(AuthError::UnknownKey),
    }
}

/// A presigned (query-string auth) URL path and query for `method` on `raw_path`, signing `host` only
/// with an unsigned payload, valid `expires_secs` from `now_unix_secs`. `raw_path` is already
/// percent-encoded the way it will be sent. Returns `path?query`; the caller prefixes scheme and host.
pub fn presign(
    creds: &Credentials,
    method: &str,
    host: &str,
    raw_path: &str,
    region: &str,
    now_unix_secs: u64,
    expires_secs: u64,
) -> String {
    let amz_date = format_iso_basic(now_unix_secs);
    let date = &amz_date[..8];
    let mut credential = String::new();
    aws_encode(format!("{}/{date}/{region}/s3/aws4_request", creds.access_key).as_bytes(), &mut credential);
    let query = format!(
        "X-Amz-Algorithm={ALGORITHM}&X-Amz-Credential={credential}&X-Amz-Date={amz_date}&X-Amz-Expires={expires_secs}&X-Amz-SignedHeaders=host"
    );
    let headers = [("host".to_string(), host.to_string())];
    let canonical = canonical_request(method, raw_path, &query, &headers, &["host".to_string()], "UNSIGNED-PAYLOAD")
        .expect("the query and headers built here are well formed");
    let d = signature_for(&creds.secret_key, &amz_date, date, region, &canonical);
    let signature = hex_encode(&hmac(&d.key, d.string_to_sign.as_bytes()));
    let path = if raw_path.is_empty() { "/" } else { raw_path };
    format!("{path}?{query}&X-Amz-Signature={signature}")
}

/// Percent-encodes one path segment as SigV4 expects (unreserved characters kept).
pub fn encode_segment(segment: &str) -> String {
    let mut out = String::new();
    aws_encode(segment.as_bytes(), &mut out);
    out
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = (a.len() != b.len()) as u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn parse_authorization(auth: &str) -> Result<Parsed<'_>, AuthError> {
    let rest = auth
        .trim()
        .strip_prefix(ALGORITHM)
        .ok_or(AuthError::Malformed)?;
    if !rest.starts_with([' ', '\t']) {
        return Err(AuthError::Malformed);
    }
    let (mut cred, mut signed, mut sig) = (None, None, None);
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            cred = Some(v);
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed = Some(v);
        } else if let Some(v) = part.strip_prefix("Signature=") {
            sig = Some(v);
        }
    }
    let (cred, signed, sig) = match (cred, signed, sig) {
        (Some(c), Some(s), Some(g)) => (c, s, g),
        _ => return Err(AuthError::Malformed),
    };

    let scope: Vec<&str> = cred.split('/').collect();
    if scope.len() != 5 || scope[2].is_empty() || scope[1].len() != 8 {
        return Err(AuthError::Malformed);
    }
    if !scope[1].bytes().all(|b| b.is_ascii_digit()) {
        return Err(AuthError::Malformed);
    }
    if scope[3] != "s3" || scope[4] != "aws4_request" {
        return Err(AuthError::Malformed);
    }

    let signed: Vec<String> = signed
        .split(';')
        .map(|h| h.trim().to_ascii_lowercase())
        .collect();
    if signed.iter().any(|h| h.is_empty()) {
        return Err(AuthError::Malformed);
    }
    let signature = hex_decode(sig).ok_or(AuthError::Malformed)?;
    Ok(Parsed {
        access_key: scope[0],
        date: scope[1],
        region: scope[2],
        signed,
        signature,
    })
}

/// Returns the ISO 8601 basic timestamp (`YYYYMMDDTHHMMSSZ`) and its unix seconds,
/// taken from x-amz-date or, failing that, an RFC 1123 Date header.
fn request_time(headers: &[(String, String)]) -> Result<(String, u64), AuthError> {
    if let Some(v) = header(headers, "x-amz-date") {
        let v = v.trim();
        let secs = parse_iso_basic(v).ok_or(AuthError::Malformed)?;
        return Ok((v.to_string(), secs));
    }
    let v = header(headers, "date").ok_or(AuthError::Malformed)?;
    let secs = parse_rfc1123(v.trim()).ok_or(AuthError::Malformed)?;
    Ok((format_iso_basic(secs), secs))
}

fn num(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn to_unix(y: u64, mo: u64, d: u64, h: u64, mi: u64, s: u64) -> Option<u64> {
    if !(1970..=9999).contains(&y) || !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    if h > 23 || mi > 59 || s > 60 {
        return None;
    }
    // Days-from-civil (Howard Hinnant).
    let (y, m) = if mo <= 2 { (y - 1, mo + 9) } else { (y, mo - 3) };
    let era = y / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(days * 86400 + h * 3600 + mi * 60 + s)
}

fn parse_iso_basic(v: &str) -> Option<u64> {
    if v.len() != 16 || !v.is_ascii() || v.as_bytes()[8] != b'T' || v.as_bytes()[15] != b'Z' {
        return None;
    }
    to_unix(
        num(&v[0..4])?,
        num(&v[4..6])?,
        num(&v[6..8])?,
        num(&v[9..11])?,
        num(&v[11..13])?,
        num(&v[13..15])?,
    )
}

fn parse_rfc1123(v: &str) -> Option<u64> {
    if !v.is_ascii() {
        return None;
    }
    // "Fri, 24 May 2013 00:00:00 GMT"
    let mut it = v.split_whitespace();
    let _day_name = it.next()?;
    let d = num(it.next()?)?;
    let mon = it.next()?;
    let y = num(it.next()?)?;
    let hms = it.next()?;
    if it.next()? != "GMT" || it.next().is_some() {
        return None;
    }
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let mo = MONTHS.iter().position(|m| m.eq_ignore_ascii_case(mon))? as u64 + 1;
    let mut t = hms.split(':');
    let (h, mi, s) = (num(t.next()?)?, num(t.next()?)?, num(t.next()?)?);
    if t.next().is_some() {
        return None;
    }
    to_unix(y, mo, d, h, mi, s)
}

fn format_iso_basic(secs: u64) -> String {
    let days = secs / 86400;
    let rem = secs % 86400;
    let z = days as i64 + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + (m <= 2) as i64;
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if b.len() % 2 != 0 || b.is_empty() {
        return None;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    b.chunks(2)
        .map(|p| Some(nib(p[0])? << 4 | nib(p[1])?))
        .collect()
}

fn hex_encode(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn pct_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let h = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
                out.push(u8::from_str_radix(h, 16).ok()?);
                i += 3;
                continue;
            }
            // S3 treats '+' in a query string as a space.
            b'+' => out.push(b' '),
            c => out.push(c),
        }
        i += 1;
    }
    Some(out)
}

fn aws_encode(b: &[u8], out: &mut String) {
    for &c in b {
        if c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b'~') {
            out.push(c as char);
        } else {
            out.push_str(&format!("%{c:02X}"));
        }
    }
}

fn canonical_query(raw: &str) -> Result<String, AuthError> {
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for kv in raw.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        pairs.push((
            pct_decode(k).ok_or(AuthError::Malformed)?,
            pct_decode(v).ok_or(AuthError::Malformed)?,
        ));
    }
    pairs.sort();
    let mut out = String::new();
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        aws_encode(k, &mut out);
        out.push('=');
        aws_encode(v, &mut out);
    }
    Ok(out)
}

fn canonical_request(
    method: &str,
    raw_path: &str,
    raw_query: &str,
    headers: &[(String, String)],
    signed: &[String],
    payload_hash: &str,
) -> Result<String, AuthError> {
    let mut names: Vec<&String> = signed.iter().collect();
    names.sort();
    names.dedup();
    let mut canon_headers = String::new();
    for name in &names {
        let vals: Vec<String> = headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect();
        if vals.is_empty() {
            return Err(AuthError::Malformed);
        }
        canon_headers.push_str(name);
        canon_headers.push(':');
        canon_headers.push_str(&vals.join(","));
        canon_headers.push('\n');
    }
    let path = if raw_path.is_empty() { "/" } else { raw_path };
    Ok(format!(
        "{method}\n{path}\n{}\n{canon_headers}\n{}\n{payload_hash}",
        canonical_query(raw_query)?,
        signed.join(";"),
    ))
}

struct Derived {
    key: Vec<u8>,
    string_to_sign: String,
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    // HMAC accepts any key length, so new_from_slice cannot fail.
    let mut m = HmacSha256::new_from_slice(key).expect("hmac key length is unrestricted");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

fn signature_for(secret: &str, amz_date: &str, date: &str, region: &str, canonical: &str) -> Derived {
    let k = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k = hmac(&k, region.as_bytes());
    let k = hmac(&k, b"s3");
    let key = hmac(&k, b"aws4_request");
    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{date}/{region}/s3/aws4_request\n{}",
        hex_encode(&Sha256::digest(canonical.as_bytes()))
    );
    Derived { key, string_to_sign }
}

#[cfg(test)]
mod tests {
    use super::*;

    const AK: &str = "AKIAIOSFODNN7EXAMPLE";
    const SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const EMPTY_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const T0: u64 = 1_369_353_600; // 2013-05-24T00:00:00Z

    fn creds() -> Credentials {
        Credentials { access_key: AK.into(), secret_key: SK.into() }
    }

    fn h(k: &str, v: &str) -> (String, String) {
        (k.to_string(), v.to_string())
    }

    /// Signs like a client: returns the headers plus an Authorization header.
    fn sign(
        method: &str,
        path: &str,
        query: &str,
        mut headers: Vec<(String, String)>,
        amz_date: &str,
        signed: &[&str],
    ) -> Vec<(String, String)> {
        headers.push(h("x-amz-date", amz_date));
        let signed: Vec<String> = signed.iter().map(|s| s.to_string()).collect();
        let canonical =
            canonical_request(method, path, query, &headers, &signed, EMPTY_SHA).unwrap();
        let date = &amz_date[..8];
        let d = signature_for(SK, amz_date, date, "us-east-1", &canonical);
        let sig = hex_encode(&hmac(&d.key, d.string_to_sign.as_bytes()));
        headers.push(h(
            "Authorization",
            &format!(
                "{ALGORITHM} Credential={AK}/{date}/us-east-1/s3/aws4_request, SignedHeaders={}, Signature={sig}",
                signed.join(";")
            ),
        ));
        headers
    }

    fn base(extra: Vec<(String, String)>) -> Vec<(String, String)> {
        let mut v = vec![h("Host", "localhost:9000"), h("x-amz-content-sha256", EMPTY_SHA)];
        v.extend(extra);
        v
    }

    const SIGNED: &[&str] = &["host", "x-amz-content-sha256", "x-amz-date"];

    fn ok_req(query: &str) -> Vec<(String, String)> {
        sign("GET", "/bucket/key.parquet", query, base(vec![]), "20130524T000000Z", SIGNED)
    }

    #[test]
    fn aws_documented_example() {
        let headers = vec![
            h("Host", "examplebucket.s3.amazonaws.com"),
            h("Range", "bytes=0-9"),
            h("x-amz-content-sha256", EMPTY_SHA),
            h("x-amz-date", "20130524T000000Z"),
            h(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,\
                 SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,\
                 Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
            ),
        ];
        assert_eq!(verify(&creds(), "GET", "/test.txt", "", &headers, T0), Ok(()));
    }

    #[test]
    fn presign_matches_the_aws_documented_example() {
        // "Authenticating Requests: Using Query Parameters", GET examplebucket/test.txt for 86400 s.
        let url = presign(&creds(), "GET", "examplebucket.s3.amazonaws.com", "/test.txt", "us-east-1", T0, 86_400);
        assert!(
            url.ends_with("&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"),
            "{url}"
        );
        assert!(url.starts_with("/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&"));
    }

    #[test]
    fn round_trip_with_sorted_encoded_query() {
        let q = "prefix=hour%3D2024-01-01T00%2F&list-type=2&delimiter=%2F&continuation-token=a%2Bb%3D%3D";
        let hs = ok_req(q);
        assert_eq!(verify(&creds(), "GET", "/bucket/key.parquet", q, &hs, T0), Ok(()));
        // Client may order and encode the query differently without changing meaning.
        let q2 = "continuation-token=a%2Bb%3D%3D&delimiter=/&list-type=2&prefix=hour%3D2024-01-01T00/";
        assert_eq!(verify(&creds(), "GET", "/bucket/key.parquet", q2, &hs, T0), Ok(()));
    }

    #[test]
    fn query_canonicalisation() {
        assert_eq!(
            canonical_query("prefix=a/b=c&delimiter=/&x&b=2&b=1").unwrap(),
            "b=1&b=2&delimiter=%2F&prefix=a%2Fb%3Dc&x="
        );
        assert_eq!(canonical_query("").unwrap(), "");
        assert_eq!(canonical_query("a=%zz"), Err(AuthError::Malformed));
    }

    #[test]
    fn header_value_normalisation_and_duplicates() {
        let signed = ["host", "x-amz-content-sha256", "x-amz-date", "x-foo"];
        let hs = sign(
            "GET", "/b/k", "",
            base(vec![h("X-Foo", "  a   b "), h("x-foo", "c")]),
            "20130524T000000Z", &signed,
        );
        assert_eq!(verify(&creds(), "GET", "/b/k", "", &hs, T0), Ok(()));
    }

    #[test]
    fn rejects_each_error() {
        let c = creds();
        let good = ok_req("");
        let path = "/bucket/key.parquet";

        let no_auth: Vec<_> = good.iter().filter(|(k, _)| k != "Authorization").cloned().collect();
        assert_eq!(verify(&c, "GET", path, "", &no_auth, T0), Err(AuthError::Missing));

        let other = Credentials { access_key: "AKOTHER".into(), secret_key: SK.into() };
        assert_eq!(verify(&other, "GET", path, "", &good, T0), Err(AuthError::UnknownKey));

        assert_eq!(verify(&c, "GET", path, "", &good, T0 + 901), Err(AuthError::Expired));
        assert_eq!(verify(&c, "GET", path, "", &good, T0 - 901), Err(AuthError::Expired));
        assert_eq!(verify(&c, "GET", path, "", &good, T0 + 900), Ok(()));
        assert_eq!(verify(&c, "GET", path, "", &good, T0 - 900), Ok(()));

        assert_eq!(verify(&c, "HEAD", path, "", &good, T0), Err(AuthError::BadSignature));
        assert_eq!(verify(&c, "GET", "/bucket/other", "", &good, T0), Err(AuthError::BadSignature));
        assert_eq!(verify(&c, "GET", path, "a=b", &good, T0), Err(AuthError::BadSignature));

        let mut tampered = good.clone();
        tampered.iter_mut().find(|(k, _)| k == "Host").unwrap().1 = "evil".into();
        assert_eq!(verify(&c, "GET", path, "", &tampered, T0), Err(AuthError::BadSignature));
    }

    #[test]
    fn rejects_malformed_structure() {
        let c = creds();
        let path = "/bucket/key.parquet";
        let good = ok_req("");
        let with_auth = |f: &dyn Fn(&str) -> String| {
            good.iter()
                .map(|(k, v)| if k == "Authorization" { (k.clone(), f(v)) } else { (k.clone(), v.clone()) })
                .collect::<Vec<_>>()
        };
        for bad in [
            with_auth(&|a| a.replace("/s3/", "/ec2/")),
            with_auth(&|a| a.replace("aws4_request,", "aws5_request,")),
            with_auth(&|a| a.replace("/20130524/", "/20130523/")),
            with_auth(&|a| a.replace("host;", "")),
            with_auth(&|a| a.replace(";x-amz-date,", ",")),
            with_auth(&|a| a.replace("AWS4-HMAC-SHA256", "AWS4-HMAC-SHA1")),
        ] {
            assert_eq!(verify(&c, "GET", path, "", &bad, T0), Err(AuthError::Malformed));
        }
        let no_hash: Vec<_> = good.iter().filter(|(k, _)| k != "x-amz-content-sha256").cloned().collect();
        assert_eq!(verify(&c, "GET", path, "", &no_hash, T0), Err(AuthError::Malformed));
    }

    #[test]
    fn signed_header_omitted_from_signature_fails() {
        // Client signed host only plus the two required; server-side extra header is ignored,
        // but a header listed as signed yet absent from the request is malformed.
        let signed = ["host", "x-amz-content-sha256", "x-amz-date", "range"];
        let hs = sign("GET", "/b/k", "", base(vec![h("Range", "bytes=0-9")]), "20130524T000000Z", &signed);
        assert_eq!(verify(&creds(), "GET", "/b/k", "", &hs, T0), Ok(()));
        let stripped: Vec<_> = hs.iter().filter(|(k, _)| k != "Range").cloned().collect();
        assert_eq!(verify(&creds(), "GET", "/b/k", "", &stripped, T0), Err(AuthError::Malformed));
        let mut changed = hs.clone();
        changed.iter_mut().find(|(k, _)| k == "Range").unwrap().1 = "bytes=0-99".into();
        assert_eq!(verify(&creds(), "GET", "/b/k", "", &changed, T0), Err(AuthError::BadSignature));
    }

    #[test]
    fn date_header_fallback() {
        let mut hs = base(vec![h("Date", "Fri, 24 May 2013 00:00:00 GMT")]);
        let signed: Vec<String> = ["date", "host", "x-amz-content-sha256"].iter().map(|s| s.to_string()).collect();
        let canonical = canonical_request("GET", "/b/k", "", &hs, &signed, EMPTY_SHA).unwrap();
        let d = signature_for(SK, "20130524T000000Z", "20130524", "us-east-1", &canonical);
        let sig = hex_encode(&hmac(&d.key, d.string_to_sign.as_bytes()));
        hs.push(h("Authorization", &format!(
            "{ALGORITHM} Credential={AK}/20130524/us-east-1/s3/aws4_request, SignedHeaders=date;host;x-amz-content-sha256, Signature={sig}")));
        assert_eq!(verify(&creds(), "GET", "/b/k", "", &hs, T0), Ok(()));
        assert_eq!(format_iso_basic(T0), "20130524T000000Z");
    }

    #[test]
    fn never_panics_on_garbage() {
        let c = creds();
        let fixed = [
            "", " ", "AWS4-HMAC-SHA256", "AWS4-HMAC-SHA256 ", "AWS4-HMAC-SHA256 Credential=",
            "AWS4-HMAC-SHA256 Credential=,SignedHeaders=,Signature=",
            "AWS4-HMAC-SHA256 Credential=a/b/c/d/e,SignedHeaders=host,Signature=zz",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/2013052\u{e9}/us/s3/aws4_request,SignedHeaders=host;;,Signature=abc",
            "AWS4-HMAC-SHA256 Credential=////,,,,,;;;;,Signature=0",
            "\u{1F600}\u{1F600}\u{1F600}", "AWS4-HMAC-SHA256 Credential=%%%%%/%/%/%/%",
        ];
        let dates = ["", "x", "2013052", "20130524T000000", "2013\u{e9}524T000000Z", "99999999T999999Z", "Fri, 99 Foo 2013 99:99:99 GMT"];
        let mut seed = 0x9E3779B97F4A7C15u64;
        let mut rnd = || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };
        let mut auths: Vec<String> = fixed.iter().map(|s| s.to_string()).collect();
        for _ in 0..500 {
            let n = (rnd() % 120) as usize;
            let bytes: Vec<u8> = (0..n).map(|_| (rnd() % 256) as u8).collect();
            auths.push(String::from_utf8_lossy(&bytes).into_owned());
            let alphabet = b"=,/; AWS4-HMAC-SHA256CredentialSignedHeaders0123456789abcdef";
            auths.push((0..n).map(|_| alphabet[(rnd() % alphabet.len() as u64) as usize] as char).collect());
        }
        for a in &auths {
            for d in dates {
                let hs = vec![h("Authorization", a), h("x-amz-date", d), h("Host", "x")];
                let _ = verify(&c, "GET", "/p", "a=%", &hs, T0);
                let _ = verify(&c, "\u{0}", "", "%%&=&=%zz%", &hs, u64::MAX);
                let _ = verify(&c, "GET", "/p", "", &hs, 0);
            }
        }
    }
}
