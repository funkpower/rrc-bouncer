// SPDX-License-Identifier: LicenseRef-Reticulum
//! The user's identity and the local hubs' own.

use std::path::Path;

use rns_identity::destination::Destination;
use rns_identity::identity::Identity;

/// The RRC hub destination name (aspects `rrc`, `hub`).
pub const HUB_ASPECT: &str = "rrc.hub";

/// The `rrc.hub` address of a hub with this identity.
pub fn hub_address(identity_hash: &[u8; 16]) -> [u8; 16] {
    Destination::hash_from_name_and_identity(HUB_ASPECT, Some(identity_hash))
}

/// Load a 64-byte Reticulum identity file.
pub fn load_identity(path: &Path) -> Result<Identity, String> {
    Identity::from_file(path).map_err(|e| format!("could not load the identity {}: {e}", path.display()))
}

/// Create a directory (and its parents) only the owner can open. A
/// directory that is already there is set to mode 0700 too.
pub fn create_private_dir(dir: &Path) -> Result<(), String> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    Ok(())
}

/// Write `identity` to `path` (mode 0600) unless a file is already there.
/// The key goes to a temporary file first and is then hard-linked to `path`,
/// so the file appears whole and only once; returns false, leaving the
/// existing file alone, when another process got there first.
fn create_identity_file(path: &Path, identity: &Identity) -> Result<bool, String> {
    use std::io::Write;
    let fail = |e: std::io::Error| format!("could not write the hub identity {}: {e}", path.display());
    let key = identity
        .get_private_key()
        .ok_or_else(|| format!("could not write the hub identity {}: no private key", path.display()))?;
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}.{:016x}.tmp", std::process::id(), rand::random::<u64>()));
    let tmp = path.with_file_name(name);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let written = options.open(&tmp).and_then(|mut file| {
        file.write_all(&*key)?;
        file.sync_all()
    });
    let linked = written.and_then(|()| std::fs::hard_link(&tmp, path));
    let _ = std::fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(fail(e)),
    }
}

/// The identity of a local hub, created (mode 0600) the first time. It is
/// never the user's own: a client linking to it identifies as the user, and
/// the hub's own identity is what tells its envelopes apart. When two
/// processes (`hashes` and `run`, say) create it at once, both end up with
/// the one that was written first.
pub fn load_or_create_hub_identity(path: &Path, user: &Identity) -> Result<Identity, String> {
    if let Some(dir) = path.parent() {
        create_private_dir(dir)?;
    }
    let identity = if path.exists() {
        load_identity(path)?
    } else {
        let identity = Identity::new();
        if create_identity_file(path, &identity)? {
            tracing::info!("created the local hub identity {}", path.display());
            identity
        } else {
            load_identity(path)?
        }
    };
    if identity.hash == user.hash {
        return Err(format!(
            "{} is the user's own identity; a local hub needs one of its own (remove the file)",
            path.display()
        ));
    }
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hub_identities_are_created_private_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let user = Identity::new();
        let path = dir.path().join("state/hubs/082f8e4293d19281108a5d4faa52dcbe.identity");
        let first = load_or_create_hub_identity(&path, &user).unwrap();
        assert_eq!(std::fs::read(&path).unwrap().len(), 64);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&path), 0o600);
            assert_eq!(mode(path.parent().unwrap()), 0o700);
        }
        let again = load_or_create_hub_identity(&path, &user).unwrap();
        assert_eq!(again.hash, first.hash);
        assert_eq!(hub_address(&again.hash), hub_address(&first.hash));
        assert_ne!(first.hash, user.hash);
    }

    #[test]
    fn concurrent_creators_get_the_same_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state/hubs/082f8e4293d19281108a5d4faa52dcbe.identity");
        let user = Identity::new();
        let hashes: Vec<[u8; 16]> = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| load_or_create_hub_identity(&path, &user).unwrap().hash))
                .collect();
            threads.into_iter().map(|t| t.join().unwrap()).collect()
        });
        assert!(hashes.iter().all(|h| *h == hashes[0]));
        assert_eq!(load_identity(&path).unwrap().hash, hashes[0]);
        // No temporary files are left behind.
        assert_eq!(std::fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
    }

    #[test]
    fn an_identity_file_already_there_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hub.identity");
        let existing = Identity::new();
        existing.to_file(&path).unwrap();
        assert!(!create_identity_file(&path, &Identity::new()).unwrap());
        assert_eq!(load_identity(&path).unwrap().hash, existing.hash);
    }

    #[cfg(unix)]
    #[test]
    fn existing_directories_are_made_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let hubs = state.join("hubs");
        std::fs::create_dir_all(&hubs).unwrap();
        for d in [&state, &hubs] {
            std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        create_private_dir(&state).unwrap();
        load_or_create_hub_identity(&hubs.join("hub.identity"), &Identity::new()).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&state), 0o700);
        assert_eq!(mode(&hubs), 0o700);
    }

    #[test]
    fn the_users_identity_is_not_a_hub_identity() {
        let dir = tempfile::tempdir().unwrap();
        let user = Identity::new();
        let path = dir.path().join("hub.identity");
        user.to_file(&path).unwrap();
        assert!(load_or_create_hub_identity(&path, &user).is_err());
    }

    #[test]
    fn a_broken_identity_file_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hub.identity");
        std::fs::write(&path, b"short").unwrap();
        assert!(load_or_create_hub_identity(&path, &Identity::new()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"short");
        assert!(load_identity(&dir.path().join("missing")).is_err());
    }

    #[test]
    fn identity_files_load_and_address_like_python_reticulum() {
        // A key written by Python RNS (`RNS.Identity.to_file`): 32 bytes of
        // X25519 private key, then 32 of Ed25519.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity");
        let key: Vec<u8> = (0u8..64).collect();
        std::fs::write(&path, &key).unwrap();
        let identity = load_identity(&path).unwrap();
        assert_eq!(identity.get_private_key().unwrap().as_slice(), key.as_slice());
        // Checked against Python RNS:
        // RNS.Identity.from_bytes(bytes(range(64))).hash and
        // RNS.Destination.hash(identity, "rrc", "hub").
        assert_eq!(hex::encode(identity.hash), IDENTITY_HASH);
        assert_eq!(hex::encode(hub_address(&identity.hash)), HUB_ADDRESS);
    }

    const IDENTITY_HASH: &str = "aca31af0441d81dbec71e82da0b4b5f5";
    const HUB_ADDRESS: &str = "5bc30a2aac5fc4b925c73b2a37c54ff0";
}
