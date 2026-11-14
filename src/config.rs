// SPDX-License-Identifier: LicenseRef-Reticulum
//! The configuration file (TOML).

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

/// The whole file, as written.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The user's Reticulum identity (a 64-byte private key file).
    pub identity: PathBuf,
    /// Where the bouncer keeps its local hub identities, room sets, history,
    /// status file and the Reticulum config it writes.
    pub state_dir: PathBuf,
    /// `error`, `warn`, `info`, `debug` or `trace`, or a full tracing filter.
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub reticulum: ReticulumSection,
    /// Announce the local hubs.
    #[serde(default)]
    pub announce: bool,
    /// Minutes between announces, with `announce = true`.
    #[serde(default = "default_announce_minutes")]
    pub announce_interval_minutes: u64,
    /// Identity hashes (32 hex characters) allowed to use the local hubs
    /// besides the user's own.
    #[serde(default)]
    pub allowed_identities: Vec<String>,
    /// A client's PART also parts upstream.
    #[serde(default)]
    pub part_upstream: bool,
    #[serde(default)]
    pub history: HistorySection,
    /// The upstream hubs (`[[hub]]`).
    #[serde(default)]
    pub hub: Vec<HubSection>,
}

/// How to reach Reticulum. Exactly one of `shared_instance`, `tcp` or
/// `config_dir`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReticulumSection {
    /// Attach to the local shared instance (an `rnsd` on this host).
    #[serde(default)]
    pub shared_instance: bool,
    /// The shared instance's name (`instance_name` in its config).
    #[serde(default)]
    pub instance_name: Option<String>,
    /// A `TCPServerInterface` to connect to, as `host:port`.
    #[serde(default)]
    pub tcp: Option<String>,
    /// A complete Reticulum config directory, used as it is.
    #[serde(default)]
    pub config_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistorySection {
    /// Days a room's history is kept.
    #[serde(default = "default_history_days")]
    pub days: u64,
    /// Most megabytes of history kept per upstream hub.
    #[serde(default = "default_history_mb")]
    pub max_mb: u64,
    /// How far back the replay on JOIN reaches.
    #[serde(default = "default_replay_hours")]
    pub replay_hours: u64,
    /// Most envelopes replayed on JOIN (0: no replay).
    #[serde(default = "default_replay_max")]
    pub replay_max: usize,
}

impl Default for HistorySection {
    fn default() -> Self {
        Self {
            days: default_history_days(),
            max_mb: default_history_mb(),
            replay_hours: default_replay_hours(),
            replay_max: default_replay_max(),
        }
    }
}

/// One upstream hub.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubSection {
    /// The hub's `rrc.hub` destination hash.
    pub address: String,
    /// Rooms to be in from the start.
    #[serde(default)]
    pub rooms: Vec<String>,
    /// `K_NICK` on the bouncer's own envelopes.
    #[serde(default)]
    pub nick: Option<String>,
}

fn default_log_level() -> String {
    "info".into()
}
fn default_announce_minutes() -> u64 {
    360
}
fn default_history_days() -> u64 {
    30
}
fn default_history_mb() -> u64 {
    100
}
fn default_replay_hours() -> u64 {
    24
}
fn default_replay_max() -> usize {
    100
}

/// How the bouncer reaches Reticulum, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RnsAccess {
    Shared { instance_name: String },
    Tcp { host: String, port: u16 },
    ConfigDir(PathBuf),
}

/// `[history]`, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySettings {
    pub retention: Duration,
    /// Per upstream hub.
    pub max_bytes: u64,
    pub replay_window: Duration,
    pub replay_max: usize,
}

/// One `[[hub]]`, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubSettings {
    pub address: [u8; 16],
    /// Lower case, trimmed, without repeats (rrcd's own room names).
    pub rooms: Vec<String>,
    pub nick: Option<String>,
}

/// The configuration, checked and with paths resolved.
#[derive(Debug, Clone)]
pub struct Settings {
    pub identity: PathBuf,
    pub state_dir: PathBuf,
    pub log_level: String,
    pub rns: RnsAccess,
    pub announce: bool,
    pub announce_interval: Duration,
    pub allowed_identities: Vec<[u8; 16]>,
    pub part_upstream: bool,
    pub history: HistorySettings,
    pub hubs: Vec<HubSettings>,
}

/// Parse a 16-byte hash written as 32 hex characters (`<` `>` and `:`
/// tolerated, as Reticulum tools print them).
pub fn parse_hash(text: &str) -> Option<[u8; 16]> {
    let clean: String = text.chars().filter(|c| !matches!(c, '<' | '>' | ':' | ' ')).collect();
    hex::decode(clean).ok()?.try_into().ok()
}

/// A room name as rrcd keeps it: trimmed and lower case.
pub fn normalize_room(room: &str) -> Option<String> {
    let room = room.trim().to_lowercase();
    (!room.is_empty()).then_some(room)
}

/// `~/` at the start of a path is the home directory.
fn expand(path: &Path) -> PathBuf {
    match path.strip_prefix("~") {
        Ok(rest) => std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default()
            .join(rest),
        Err(_) => path.to_path_buf(),
    }
}

fn rns_access(r: &ReticulumSection, resolve: impl Fn(&Path) -> PathBuf) -> Result<RnsAccess, String> {
    let chosen = [r.shared_instance, r.tcp.is_some(), r.config_dir.is_some()]
        .iter()
        .filter(|c| **c)
        .count();
    if chosen != 1 {
        return Err("[reticulum]: set exactly one of shared_instance = true, tcp = \"host:port\" or config_dir".into());
    }
    if r.instance_name.is_some() && !r.shared_instance {
        return Err("[reticulum]: instance_name only applies with shared_instance = true".into());
    }
    if r.shared_instance {
        let name = r.instance_name.clone().unwrap_or_else(|| "default".into());
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)) {
            return Err(format!(
                "[reticulum]: instance_name {name:?} is not a valid instance name"
            ));
        }
        Ok(RnsAccess::Shared { instance_name: name })
    } else if let Some(tcp) = &r.tcp {
        let (host, port) = tcp
            .rsplit_once(':')
            .ok_or_else(|| format!("[reticulum]: tcp = {tcp:?} is not host:port"))?;
        let port = port
            .parse::<u16>()
            .ok()
            .filter(|p| *p > 0)
            .ok_or_else(|| format!("[reticulum]: bad port in {tcp:?}"))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() || host.contains(['\n', '\r', '[', ']']) {
            return Err(format!("[reticulum]: bad host in {tcp:?}"));
        }
        Ok(RnsAccess::Tcp {
            host: host.to_string(),
            port,
        })
    } else {
        Ok(RnsAccess::ConfigDir(resolve(
            r.config_dir.as_deref().unwrap_or(Path::new("")),
        )))
    }
}

impl Config {
    pub fn parse(text: &str) -> Result<Self, String> {
        toml::from_str(text).map_err(|e| e.to_string())
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Check it, resolving paths relative to the directory the file is in.
    pub fn settings(&self, base: &Path) -> Result<Settings, String> {
        let resolve = |path: &Path| {
            let path = expand(path);
            if path.is_absolute() { path } else { base.join(path) }
        };
        let rns = rns_access(&self.reticulum, resolve)?;
        if self.announce_interval_minutes == 0 {
            return Err("announce_interval_minutes must be at least 1".into());
        }
        let allowed_identities = self
            .allowed_identities
            .iter()
            .map(|h| parse_hash(h).ok_or_else(|| format!("allowed_identities: {h:?} is not 32 hex characters")))
            .collect::<Result<Vec<_>, _>>()?;
        let h = &self.history;
        if h.days == 0 {
            return Err("[history]: days must be at least 1".into());
        }
        if h.max_mb == 0 {
            return Err("[history]: max_mb must be at least 1".into());
        }
        if h.replay_max > crate::relay::MAX_HISTORY {
            return Err(format!(
                "[history]: replay_max must be at most {} (it is {})",
                crate::relay::MAX_HISTORY,
                h.replay_max
            ));
        }
        if self.hub.is_empty() {
            return Err("no [[hub]]: name at least one upstream hub".into());
        }
        let mut hubs: Vec<HubSettings> = Vec::new();
        for hub in &self.hub {
            let address = parse_hash(&hub.address)
                .ok_or_else(|| format!("[[hub]]: address {:?} is not 32 hex characters", hub.address))?;
            if hubs.iter().any(|h| h.address == address) {
                return Err(format!("[[hub]]: {} is listed twice", hex::encode(address)));
            }
            let mut rooms: Vec<String> = Vec::new();
            for room in &hub.rooms {
                let room =
                    normalize_room(room).ok_or_else(|| format!("[[hub]] {}: empty room name", hex::encode(address)))?;
                if !rooms.contains(&room) {
                    rooms.push(room);
                }
            }
            hubs.push(HubSettings {
                address,
                rooms,
                nick: hub
                    .nick
                    .as_deref()
                    .map(str::trim)
                    .filter(|n| !n.is_empty())
                    .map(String::from),
            });
        }
        Ok(Settings {
            identity: resolve(&self.identity),
            state_dir: resolve(&self.state_dir),
            log_level: self.log_level.clone(),
            rns,
            announce: self.announce,
            announce_interval: Duration::from_secs(self.announce_interval_minutes.saturating_mul(60)),
            allowed_identities,
            part_upstream: self.part_upstream,
            history: HistorySettings {
                retention: Duration::from_secs(h.days.saturating_mul(86_400)),
                max_bytes: h.max_mb.saturating_mul(1_000_000),
                replay_window: Duration::from_secs(h.replay_hours.saturating_mul(3600)),
                replay_max: h.replay_max,
            },
            hubs,
        })
    }
}

impl Settings {
    /// Load and check the file at `path`.
    pub fn load(path: &Path) -> Result<Self, String> {
        let base = path.parent().map(Path::to_path_buf).unwrap_or_default();
        let base = if base.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            base
        };
        Config::load(path)?.settings(&base)
    }

    pub fn hubs_dir(&self) -> PathBuf {
        self.state_dir.join("hubs")
    }
    /// The identity of the local hub that stands for `upstream`.
    pub fn hub_identity_path(&self, upstream: &[u8; 16]) -> PathBuf {
        self.hubs_dir().join(format!("{}.identity", hex::encode(upstream)))
    }
    pub fn history_dir(&self, upstream: &[u8; 16]) -> PathBuf {
        self.state_dir.join("history").join(hex::encode(upstream))
    }
    /// The rooms added to `upstream`'s room set at run time.
    pub fn rooms_path(&self, upstream: &[u8; 16]) -> PathBuf {
        self.state_dir
            .join("rooms")
            .join(format!("{}.json", hex::encode(upstream)))
    }
    pub fn status_path(&self) -> PathBuf {
        self.state_dir.join("status.json")
    }
    /// The Reticulum config directory: the configured one, or one the
    /// bouncer writes under its state directory.
    pub fn rns_config_dir(&self) -> PathBuf {
        match &self.rns {
            RnsAccess::ConfigDir(dir) => dir.clone(),
            _ => self.state_dir.join("reticulum"),
        }
    }

    /// The Reticulum config the bouncer writes for itself (`None` when a
    /// config directory of the user's is used).
    pub fn generated_rns_config(&self) -> Option<String> {
        match &self.rns {
            RnsAccess::ConfigDir(_) => None,
            RnsAccess::Shared { instance_name } => Some(format!(
                "# Written by rrc-bouncer on each start; edit its own config instead.\n\
                 [reticulum]\n  enable_transport = No\n  share_instance = Yes\n  instance_name = {instance_name}\n  panic_on_interface_error = No\n\n\
                 [logging]\n  loglevel = 2\n\n[interfaces]\n"
            )),
            RnsAccess::Tcp { host, port } => Some(format!(
                "# Written by rrc-bouncer on each start; edit its own config instead.\n\
                 [reticulum]\n  enable_transport = No\n  share_instance = No\n  instance_name = rrc-bouncer\n  panic_on_interface_error = No\n\n\
                 [logging]\n  loglevel = 2\n\n[interfaces]\n  [[rrc-bouncer uplink]]\n    type = TCPClientInterface\n    enabled = Yes\n    target_host = {host}\n    target_port = {port}\n"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HUB: &str = "082f8e4293d19281108a5d4faa52dcbe";
    const MINIMAL: &str = "identity = \"id\"\nstate_dir = \"/var/lib/b\"\n[reticulum]\ntcp = \"127.0.0.1:4242\"\n[[hub]]\naddress = \"082f8e4293d19281108a5d4faa52dcbe\"\n";

    fn settings(text: &str) -> Result<Settings, String> {
        Config::parse(text)?.settings(Path::new("/etc/b"))
    }

    #[test]
    fn minimal_config_gets_defaults() {
        let s = settings(MINIMAL).unwrap();
        assert_eq!(s.identity, PathBuf::from("/etc/b/id"));
        assert_eq!(s.state_dir, PathBuf::from("/var/lib/b"));
        assert_eq!(s.log_level, "info");
        assert_eq!(
            s.rns,
            RnsAccess::Tcp {
                host: "127.0.0.1".into(),
                port: 4242
            }
        );
        assert!(!s.announce);
        assert_eq!(s.announce_interval, Duration::from_secs(360 * 60));
        assert!(s.allowed_identities.is_empty());
        assert!(!s.part_upstream);
        assert_eq!(
            s.history,
            HistorySettings {
                retention: Duration::from_secs(30 * 86_400),
                max_bytes: 100_000_000,
                replay_window: Duration::from_secs(24 * 3600),
                replay_max: 100,
            }
        );
        assert_eq!(
            s.hubs,
            vec![HubSettings {
                address: parse_hash(HUB).unwrap(),
                rooms: Vec::new(),
                nick: None
            }]
        );
        assert_eq!(
            s.hub_identity_path(&s.hubs[0].address),
            PathBuf::from(format!("/var/lib/b/hubs/{HUB}.identity"))
        );
        assert_eq!(
            s.history_dir(&s.hubs[0].address),
            PathBuf::from(format!("/var/lib/b/history/{HUB}"))
        );
        assert_eq!(s.rns_config_dir(), PathBuf::from("/var/lib/b/reticulum"));
        let generated = s.generated_rns_config().unwrap();
        assert!(generated.contains("share_instance = No"));
        assert!(generated.contains("target_host = 127.0.0.1"));
        assert!(generated.contains("target_port = 4242"));
    }

    #[test]
    fn the_example_config_is_valid() {
        let text = include_str!("../contrib/config.example.toml");
        let s = Config::parse(text)
            .unwrap()
            .settings(Path::new("/etc/rrc-bouncer"))
            .unwrap();
        assert_eq!(
            s.rns,
            RnsAccess::Tcp {
                host: "127.0.0.1".into(),
                port: 4242
            }
        );
        assert_eq!(s.hubs.len(), 1);
        assert_eq!(s.hubs[0].rooms, vec!["lobby".to_string()]);
        assert_eq!(s.hubs[0].nick.as_deref(), Some("yournick"));
        assert!(s.identity.ends_with(".local/share/rrc-bouncer/identity"));
        assert!(s.identity.is_absolute());
    }

    #[test]
    fn exactly_one_way_to_reticulum() {
        let hub = format!("[[hub]]\naddress = \"{HUB}\"\n");
        let none = format!("identity = \"id\"\nstate_dir = \"s\"\n{hub}");
        assert!(settings(&none).is_err());
        let two =
            format!("identity = \"id\"\nstate_dir = \"s\"\n[reticulum]\nshared_instance = true\ntcp = \"a:1\"\n{hub}");
        assert!(settings(&two).is_err());
        let name_without_shared =
            format!("identity = \"id\"\nstate_dir = \"s\"\n[reticulum]\ntcp = \"a:1\"\ninstance_name = \"x\"\n{hub}");
        assert!(settings(&name_without_shared).is_err());
        let shared = format!("identity = \"id\"\nstate_dir = \"s\"\n[reticulum]\nshared_instance = true\n{hub}");
        let s = settings(&shared).unwrap();
        assert_eq!(
            s.rns,
            RnsAccess::Shared {
                instance_name: "default".into()
            }
        );
        assert!(s.generated_rns_config().unwrap().contains("share_instance = Yes"));
        let named = format!(
            "identity = \"id\"\nstate_dir = \"s\"\n[reticulum]\nshared_instance = true\ninstance_name = \"bad name\"\n{hub}"
        );
        assert!(settings(&named).is_err());
        let dir = format!("identity = \"id\"\nstate_dir = \"s\"\n[reticulum]\nconfig_dir = \"rns\"\n{hub}");
        let s = settings(&dir).unwrap();
        assert_eq!(s.rns, RnsAccess::ConfigDir(PathBuf::from("/etc/b/rns")));
        assert_eq!(s.rns_config_dir(), PathBuf::from("/etc/b/rns"));
        assert!(s.generated_rns_config().is_none());
        for bad in ["a", "a:0", "a:x", ":1", "[]:1"] {
            let text = format!("identity = \"id\"\nstate_dir = \"s\"\n[reticulum]\ntcp = \"{bad}\"\n{hub}");
            assert!(settings(&text).is_err(), "{bad}");
        }
        let v6 = format!("identity = \"id\"\nstate_dir = \"s\"\n[reticulum]\ntcp = \"[::1]:4242\"\n{hub}");
        assert_eq!(
            settings(&v6).unwrap().rns,
            RnsAccess::Tcp {
                host: "::1".into(),
                port: 4242
            }
        );
    }

    #[test]
    fn paths_resolve_against_the_config_file() {
        let home = PathBuf::from(std::env::var_os("HOME").unwrap());
        let text = "identity = \"~/id\"\nstate_dir = \"state\"\n[reticulum]\ntcp = \"a:1\"\n[[hub]]\naddress = \"082f8e4293d19281108a5d4faa52dcbe\"\n";
        let s = settings(text).unwrap();
        assert_eq!(s.identity, home.join("id"));
        assert_eq!(s.state_dir, PathBuf::from("/etc/b/state"));
    }

    #[test]
    fn hubs_rooms_and_identities_are_checked() {
        assert!(settings("identity = \"id\"\nstate_dir = \"s\"\n[reticulum]\ntcp = \"a:1\"\n").is_err());
        let bad_hash = MINIMAL.replace(HUB, "abc");
        assert!(settings(&bad_hash).is_err());
        let twice = format!("{MINIMAL}[[hub]]\naddress = \"<{HUB}>\"\n");
        assert!(settings(&twice).is_err());
        let rooms = format!(
            "{MINIMAL}[[hub]]\naddress = \"00112233445566778899aabbccddeeff\"\nrooms = [\" Lobby \", \"lobby\", \"dev\"]\nnick = \"  \"\n"
        );
        let s = settings(&rooms).unwrap();
        assert_eq!(s.hubs[1].rooms, vec!["lobby".to_string(), "dev".to_string()]);
        assert_eq!(s.hubs[1].nick, None);
        let empty_room = format!("{MINIMAL}rooms = [\" \"]\n");
        assert!(settings(&empty_room).is_err());
        let allowed = MINIMAL.replace(
            "[reticulum]",
            "allowed_identities = [\"00112233445566778899aabbccddeeff\"]\n[reticulum]",
        );
        assert_eq!(settings(&allowed).unwrap().allowed_identities.len(), 1);
        let bad_allowed = MINIMAL.replace("[reticulum]", "allowed_identities = [\"0011\"]\n[reticulum]");
        assert!(settings(&bad_allowed).is_err());
    }

    #[test]
    fn typos_and_zeroes_are_refused() {
        let typo = MINIMAL.replace("[reticulum]", "anounce = true\n[reticulum]");
        assert!(Config::parse(&typo).is_err());
        let hub_typo = format!("{MINIMAL}room = [\"a\"]\n");
        assert!(Config::parse(&hub_typo).is_err());
        let zero_interval = MINIMAL.replace("[reticulum]", "announce_interval_minutes = 0\n[reticulum]");
        assert!(settings(&zero_interval).is_err());
        let zero_days = MINIMAL.replace("[reticulum]", "[history]\ndays = 0\n[reticulum]");
        assert!(settings(&zero_days).is_err());
        let zero_mb = MINIMAL.replace("[reticulum]", "[history]\nmax_mb = 0\n[reticulum]");
        assert!(settings(&zero_mb).is_err());
        let most = MINIMAL.replace("[reticulum]", "[history]\nreplay_max = 500\n[reticulum]");
        assert_eq!(settings(&most).unwrap().history.replay_max, 500);
        let too_many = MINIMAL.replace("[reticulum]", "[history]\nreplay_max = 501\n[reticulum]");
        assert!(
            settings(&too_many)
                .unwrap_err()
                .contains("replay_max must be at most 500")
        );
        let no_replay = MINIMAL.replace(
            "[reticulum]",
            "announce = true\npart_upstream = true\n[history]\nreplay_max = 0\nreplay_hours = 0\n[reticulum]",
        );
        let s = settings(&no_replay).unwrap();
        assert!(s.announce && s.part_upstream);
        assert_eq!((s.history.replay_max, s.history.replay_window), (0, Duration::ZERO));
    }
}
