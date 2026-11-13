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

/// Create a directory (and its parents) only the owner can open.
pub fn create_private_dir(dir: &Path) -> Result<(), String> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir).map_err(|e| format!("{}: {e}", dir.display()))
}

/// The identity of a local hub, created (mode 0600) the first time. It is
/// never the user's own: a client linking to it identifies as the user, and
/// the hub's own identity is what tells its envelopes apart.
pub fn load_or_create_hub_identity(path: &Path, user: &Identity) -> Result<Identity, String> {
    let identity = if path.exists() {
        load_identity(path)?
    } else {
        if let Some(dir) = path.parent() {
            create_private_dir(dir)?;
        }
        let identity = Identity::new();
        // Written with mode 0600 (rns-identity's atomic write).
        identity
            .to_file(path)
            .map_err(|e| format!("could not write the hub identity {}: {e}", path.display()))?;
        tracing::info!("created the local hub identity {}", path.display());
        identity
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
