// SPDX-License-Identifier: LicenseRef-Reticulum
//! The rooms the bouncer stays in on one upstream hub.
//!
//! A hub's room set is the `rooms` of its `[[hub]]` entry followed by the
//! rooms added while the bouncer runs (a client's JOIN, from M3), which are
//! kept in `state_dir/rooms/<upstream hash>.json` as `{"rooms": [...]}`.
//! The file holds only the added rooms; the config file stays the user's.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::normalize_room;
use crate::identity::create_private_dir;

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoomsFile {
    rooms: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RoomSet {
    path: PathBuf,
    config: Vec<String>,
    added: Vec<String>,
}

fn push_unique(list: &mut Vec<String>, room: String) {
    if !list.contains(&room) {
        list.push(room);
    }
}

impl RoomSet {
    /// Read the file at `path`, if there is one. Its names are normalised
    /// as rrcd normalises them (trimmed, lower case, duplicates dropped) and
    /// the file is written back if that changed it. A file that cannot be
    /// read is an error and is left as it is.
    pub fn load(path: &Path, config: &[String]) -> Result<Self, String> {
        let mut config_rooms = Vec::new();
        for room in config.iter().filter_map(|r| normalize_room(r)) {
            push_unique(&mut config_rooms, room);
        }
        let mut set = Self {
            path: path.to_path_buf(),
            config: config_rooms,
            added: Vec::new(),
        };
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(set),
            Err(e) => return Err(format!("{}: {e}", path.display())),
        };
        let file: RoomsFile = serde_json::from_str(&text).map_err(|e| {
            format!(
                "{}: {e} (fix or remove the file; the bouncer does not overwrite it)",
                path.display()
            )
        })?;
        for room in file.rooms.iter().filter_map(|r| normalize_room(r)) {
            push_unique(&mut set.added, room);
        }
        if set.added != file.rooms {
            set.save()?;
        }
        Ok(set)
    }

    /// Every room, the configured ones first.
    pub fn rooms(&self) -> Vec<String> {
        let mut all = self.config.clone();
        for room in &self.added {
            push_unique(&mut all, room.clone());
        }
        all
    }

    pub fn contains(&self, room: &str) -> bool {
        self.config.iter().chain(&self.added).any(|r| r == room)
    }

    /// Add a room and save the file. False if the set already had it.
    pub fn add(&mut self, room: &str) -> Result<bool, String> {
        let Some(room) = normalize_room(room) else {
            return Ok(false);
        };
        if self.contains(&room) {
            return Ok(false);
        }
        self.added.push(room);
        self.save()?;
        Ok(true)
    }

    /// Take a room out of the file. A room from the config file cannot be
    /// taken out here: it comes back on the next start.
    pub fn remove(&mut self, room: &str) -> Result<bool, String> {
        let Some(room) = normalize_room(room) else {
            return Ok(false);
        };
        let before = self.added.len();
        self.added.retain(|r| *r != room);
        if self.added.len() == before {
            return Ok(false);
        }
        self.save()?;
        Ok(true)
    }

    pub fn is_configured(&self, room: &str) -> bool {
        self.config.iter().any(|r| r == room)
    }

    fn save(&self) -> Result<(), String> {
        let err = |e: std::io::Error| format!("{}: {e}", self.path.display());
        if let Some(dir) = self.path.parent() {
            create_private_dir(dir)?;
        }
        let file = RoomsFile {
            rooms: self.added.clone(),
        };
        let mut data = serde_json::to_vec_pretty(&file).map_err(|e| e.to_string())?;
        data.push(b'\n');
        write_private(&self.path, &data).map_err(err)
    }
}

/// Write `data` to `path` through a temporary file (mode 0600) and a rename.
fn write_private(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    file.write_all(data)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Vec<String> {
        vec!["Lobby".into(), " lobby ".into(), "dev".into()]
    }

    #[test]
    fn without_a_file_the_set_is_the_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rooms").join("x.json");
        let set = RoomSet::load(&path, &config()).unwrap();
        assert_eq!(set.rooms(), ["lobby", "dev"]);
        assert!(!path.exists());
    }

    #[test]
    fn added_rooms_are_saved_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rooms").join("x.json");
        let mut set = RoomSet::load(&path, &config()).unwrap();
        assert!(set.add(" Side ").unwrap());
        assert!(!set.add("side").unwrap());
        assert!(!set.add("LOBBY").unwrap());
        assert!(!set.add("  ").unwrap());
        let again = RoomSet::load(&path, &config()).unwrap();
        assert_eq!(again.rooms(), ["lobby", "dev", "side"]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let mut again = again;
        assert!(again.remove("Side").unwrap());
        assert!(!again.remove("lobby").unwrap());
        assert!(again.is_configured("lobby"));
        assert_eq!(RoomSet::load(&path, &config()).unwrap().rooms(), ["lobby", "dev"]);
    }

    #[test]
    fn the_file_is_normalised_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.json");
        std::fs::write(&path, r#"{"rooms": [" Side ", "side", "", "Dev", "third"]}"#).unwrap();
        let set = RoomSet::load(&path, &config()).unwrap();
        assert_eq!(set.rooms(), ["lobby", "dev", "side", "third"]);
        let text = std::fs::read_to_string(&path).unwrap();
        let file: RoomsFile = serde_json::from_str(&text).unwrap();
        // "dev" stays in the file: it was added at run time, and taking it
        // out of the config later must not lose it.
        assert_eq!(file.rooms, ["side", "dev", "third"]);
    }

    #[test]
    fn a_broken_file_is_an_error_and_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.json");
        for text in ["{\"rooms\": [", "{\"rooms\": [1]}", "[\"a\"]", "{\"room\": []}"] {
            std::fs::write(&path, text).unwrap();
            let err = RoomSet::load(&path, &config()).unwrap_err();
            assert!(err.contains("x.json"), "{err}");
            assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
        }
    }
}
