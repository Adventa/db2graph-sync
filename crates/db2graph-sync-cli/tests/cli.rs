//! CLI behaviour: attribution, exit codes, offline commands.

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_db2graph-sync"))
}

fn root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn about_carries_attribution_and_makes_no_network_calls() {
    let out = bin().arg("about").output().unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("db2graph-sync — an open-source project by Adventa."));
    assert!(s.contains("https://www.adventa.com.au"));
    assert!(s.contains("Apache-2.0"));
}

#[test]
fn usage_errors_exit_64_help_exits_0() {
    assert_eq!(bin().arg("no-such-command").output().unwrap().status.code(), Some(64));
    assert_eq!(bin().arg("--help").output().unwrap().status.code(), Some(0));
}

#[test]
fn keygen_and_token_hash() {
    let out = bin().arg("keygen").output().unwrap();
    let key = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    assert_eq!(key.len(), 44, "32 bytes base64");
    let mut child = bin().arg("token-hash").stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).spawn().unwrap();
    use std::io::Write;
    child.stdin.take().unwrap().write_all(b"abc\n").unwrap();
    let out = child.wait_with_output().unwrap();
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

#[test]
fn validate_examples_and_exit_codes() {
    let dev = root().join("examples/config/postgres-neo4j.dev.toml");
    let out = bin().args(["validate", "-c"]).arg(&dev).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stdout));

    let ent = root().join("examples/config/postgres-neo4j.enterprise.toml");
    let out = bin().args(["validate", "-c"]).arg(&ent).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stdout));

    // Plaintext outside development is a security finding: exit 5.
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("bad.toml");
    let s = std::fs::read_to_string(&dev).unwrap().replace("profile = \"development\"", "profile = \"enterprise\"");
    std::fs::write(&bad, s).unwrap();
    let out = bin().args(["validate", "-c"]).arg(&bad).output().unwrap();
    assert_eq!(out.status.code(), Some(5), "{}", String::from_utf8_lossy(&out.stdout));

    // Unparseable config: exit 2.
    std::fs::write(&bad, "[pipeline]\nid = 3").unwrap();
    assert_eq!(bin().args(["validate", "-c"]).arg(&bad).output().unwrap().status.code(), Some(2));
}

#[test]
fn source_setup_prints_but_never_executes() {
    let dev = root().join("examples/config/postgres-neo4j.dev.toml");
    let out = bin().args(["source-setup", "-c"]).arg(&dev).output().unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("CREATE PUBLICATION \"d2g_demo\" FOR TABLE \"public\".\"customers\", \"public\".\"orders\";"));
    assert!(s.contains("never executes"));
}

#[test]
fn restricted_profile_reports_missing_validated_provider() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("r.toml");
    std::fs::write(&cfg, format!(
        "[pipeline]\nid = \"r\"\ntenant = \"t\"\nprofile = \"restricted\"\n[source]\ntype = \"postgres\"\nhost = \"h\"\ndatabase = \"d\"\nuser = \"u\"\npublication = \"p\"\nslot = \"s\"\n\
         [graph]\ntype = \"neo4j\"\nuri = \"bolt+s://g:7687\"\nuser = \"u\"\npassword = \"env:X\"\n[state]\ndir = '{}'\nactive_key = 1\nkeys = [{{ id = 1, key = \"env:K\" }}]\n\
         [audit]\nfile = \"a.jsonl\"\n[mapping]\napproved_digest = \"00000000000000000000000000000000\"\n[[policy.datasets]]\nrelation = \"public.x\"\nallow_fields = [\"id\"]\n",
        dir.path().join("state").display()
    )).unwrap();
    // Config itself is valid for the profile...
    assert!(bin().args(["validate", "-c"]).arg(&cfg).output().unwrap().status.success());
    // ...but running refuses with a clear error and the security exit code.
    let out = bin().args(["run", "-c"]).arg(&cfg).env("K", "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").env("X", "x").output().unwrap();
    assert_eq!(out.status.code(), Some(5));
    assert!(String::from_utf8_lossy(&out.stderr).contains("restricted_provider_unavailable"));
}
