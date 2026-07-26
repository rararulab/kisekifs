use std::{collections::HashSet, fs, path::PathBuf};

use serde::Deserialize;

const REGISTERED_MOUNTED_TESTS: &[&str] = &[
    "mounted::smoke::mount_create_unmount",
    "mounted::semantics::namespace_and_metadata",
    "mounted::semantics::io_boundaries",
    "mounted::semantics::descriptor_lifecycle",
    "mounted::concurrency::ordered_and_disjoint_writes",
    "mounted::lifecycle::clean_remount_and_read_only",
    "mounted::lifecycle::crash_after_fsync",
    "mounted::lifecycle::crash_after_local_flush",
    "mounted::unsupported::stable_errno_keeps_mount_alive",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupportManifest {
    version: u32,
    case:    Vec<SupportCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupportCase {
    id:             String,
    operation:      String,
    status:         SupportStatus,
    test:           Option<String>,
    expected_errno: Option<String>,
    caveat:         Option<String>,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum SupportStatus {
    Supported,
    Unsupported,
    Experimental,
}

fn workspace_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests crate is inside the workspace")
        .join(relative)
}

#[test]
fn support_manifest_is_well_formed_and_registered() {
    let raw = fs::read_to_string(workspace_path("tests/fixtures/posix-support.toml"))
        .expect("read POSIX support manifest");
    let manifest: SupportManifest = toml::from_str(&raw).expect("parse POSIX support manifest");
    assert_eq!(manifest.version, 1, "unsupported manifest schema");

    let mut ids = HashSet::new();
    let registered = REGISTERED_MOUNTED_TESTS
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let prose = fs::read_to_string(workspace_path("docs/src/posix-support.md"))
        .expect("read human POSIX support matrix");

    for case in manifest.case {
        assert!(!case.id.trim().is_empty(), "case id must not be empty");
        assert!(
            ids.insert(case.id.clone()),
            "duplicate case id: {}",
            case.id
        );
        assert!(
            !case.operation.trim().is_empty(),
            "operation must not be empty: {}",
            case.id
        );
        assert!(
            prose.contains(&format!("`{}`", case.id)),
            "case {} is missing from docs/src/posix-support.md",
            case.id
        );

        match case.status {
            SupportStatus::Supported => {
                let test = case
                    .test
                    .as_deref()
                    .unwrap_or_else(|| panic!("supported case {} needs a test", case.id));
                assert!(
                    registered.contains(test),
                    "supported case {} references unregistered test {test}",
                    case.id
                );
                assert!(
                    case.expected_errno.is_none(),
                    "supported case {} must not declare an error",
                    case.id
                );
            }
            SupportStatus::Unsupported => {
                assert!(
                    case.expected_errno.is_some(),
                    "unsupported case {} needs an expected errno",
                    case.id
                );
            }
            SupportStatus::Experimental => {
                assert!(
                    case.caveat
                        .as_deref()
                        .is_some_and(|value| !value.is_empty()),
                    "experimental case {} needs a caveat",
                    case.id
                );
            }
        }
    }
}
