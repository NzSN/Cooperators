mod common;

use std::os::unix::fs::PermissionsExt;

use cooperators::paths;

#[test]
fn ensure_layout_creates_the_design_directories_with_0700_on_root_and_run() {
    let temp = common::TempDir::new("layout");
    let root = temp.path().join("root");

    paths::ensure_layout(&root).expect("create layout");

    for relative in [
        "run",
        "agents",
        "channels",
        "queues",
        "dead",
        "docs",
        "blobs/sha256",
    ] {
        assert!(root.join(relative).is_dir(), "missing directory {relative}");
    }
    for directory in [&root, &root.join("run")] {
        let mode = std::fs::metadata(directory)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700, "{} is not 0700", directory.display());
    }
}
