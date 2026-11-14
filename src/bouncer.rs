// SPDX-License-Identifier: LicenseRef-Reticulum
//! Starting Reticulum and running the local hubs.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use ciborium::Value;
use rns_runtime::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc, watch};

use crate::config::{RnsAccess, Settings};
use crate::history::{History, HistoryStatus, SharedHistoryStatus};
use crate::identity::{HUB_ASPECT, create_private_dir, hub_address, load_identity, load_or_create_hub_identity};
use crate::relay::{self, LinkIdentities, LocalHub, Relay, RelayConfig, RelayStatus, SharedRelayStatus};
use crate::rooms::RoomSet;
use crate::upstream::{self, COMMAND_QUEUE, SharedStatus, Timings, Upstream, UpstreamStatus};

/// How long to wait after starting before the first announce, so
/// interfaces have come up.
const FIRST_ANNOUNCE_DELAY: Duration = Duration::from_secs(5);
const STATUS_INTERVAL: Duration = Duration::from_secs(60);
/// How often the uplink is checked for having (re)connected.
const UPLINK_CHECK: Duration = Duration::from_secs(10);
/// How often the path responses on the uplink are sent again. Python rnsd
/// drops a path nobody has used after 12 minutes on an instance without
/// transport and 1.75 hours with it (`UNUSED_PATH_LINGER_LEAF`,
/// `UNUSED_PATH_LINGER`), and answering a path request does not count as use.
const UPLINK_REFRESH: Duration = Duration::from_secs(5 * 60);
/// Overrides `UPLINK_REFRESH`, in seconds, for tests/e2e.py.
const UPLINK_REFRESH_ENV: &str = "RRC_BOUNCER_UPLINK_REFRESH_SECS";
/// How long each hub's history gets to write what it was given at shutdown.
const HISTORY_STOP: Duration = Duration::from_secs(5);
/// How long Reticulum gets to stop at shutdown.
const RETICULUM_STOP: Duration = Duration::from_secs(5);
/// How long the last write of status.json gets at shutdown.
const STATUS_STOP: Duration = Duration::from_secs(2);
/// The file in `state_dir` a running bouncer holds locked.
pub const LOCK_FILE: &str = "lock";
/// How long the upstream sessions get to close their links at shutdown:
/// longer than a session waits for its link to close, so each one gets to
/// write `stopped` to its status.
const SESSION_STOP: Duration = Duration::from_secs(10);
const _: () = assert!(SESSION_STOP.as_secs() > upstream::CLOSE_WAIT.as_secs());
/// The capacity of a local hub's event channels (rsReticulum's default is
/// 128): every client link's Resource advertisements, progress and ends
/// pass through them, and one that is full loses events.
const HUB_EVENTS: usize = 1024;

fn uplink_refresh() -> Duration {
    std::env::var(UPLINK_REFRESH_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&secs| secs > 0)
        .map_or(UPLINK_REFRESH, Duration::from_secs)
}

pub fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// How long until the next whole second of wall time. An announce carries
/// its emission time in whole seconds, and rnsd takes a new one for a path
/// it already has only when that time is later (`Transport.py:2519`): one
/// sent in the same second as the last is dropped. 10 ms past the boundary,
/// so rounding never lands the wake just short of it.
fn until_next_second(now: f64) -> Duration {
    Duration::from_secs_f64((now.floor() + 1.0 - now).clamp(0.0, 1.0) + 0.01)
}

/// Write `data` to `path` through a temporary file and a rename.
pub fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(data)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)
}

/// The announce app data of a hub: `{"proto": "rrc", "v": 1, "hub": name}`,
/// as rrcd writes it.
pub fn hub_app_data(name: &str) -> Vec<u8> {
    let map = Value::Map(vec![
        (Value::Text("proto".into()), Value::Text("rrc".into())),
        (Value::Text("v".into()), Value::Integer(1.into())),
        (Value::Text("hub".into()), Value::Text(name.into())),
    ]);
    let mut out = Vec::new();
    ciborium::into_writer(&map, &mut out).unwrap_or_default();
    out
}

/// The name a local hub goes by until the upstream hub's own is known.
pub fn local_hub_name(upstream: &[u8; 16]) -> String {
    format!("rrc-bouncer for {}", hex::encode(upstream))
}

/// One local hub, as `hashes` and `status` show it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubStatus {
    pub upstream: String,
    pub local_hub: String,
    /// The upstream session (M2).
    #[serde(flatten)]
    pub link: UpstreamStatus,
    /// The local hub's clients (M3).
    #[serde(flatten)]
    pub relay: RelayStatus,
    /// The hub's history on disk (M4).
    #[serde(flatten)]
    pub history: HistoryStatus,
}

/// `state_dir/status.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub pid: u32,
    pub started_at: f64,
    pub updated_at: f64,
    pub identity_hash: String,
    pub announce: bool,
    pub hubs: Vec<HubStatus>,
}

/// The user's identity hash and each local hub's address.
pub fn hubs(settings: &Settings) -> Result<(String, Vec<HubStatus>), String> {
    let identity = load_identity(&settings.identity)?;
    let mut hubs = Vec::new();
    for hub in &settings.hubs {
        let local = load_or_create_hub_identity(&settings.hub_identity_path(&hub.address), &identity)?;
        hubs.push(HubStatus {
            upstream: hex::encode(hub.address),
            local_hub: hex::encode(hub_address(&local.hash)),
            link: UpstreamStatus::default(),
            relay: RelayStatus::default(),
            history: HistoryStatus::default(),
        });
    }
    Ok((hex::encode(identity.hash), hubs))
}

/// Whether a Reticulum shared instance is listening on this host. Python
/// Reticulum and rsReticulum both listen on the abstract Unix socket
/// `\0rns/<instance_name>` on Linux.
fn shared_instance_listening(instance_name: &str) -> bool {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        #[cfg(target_os = "android")]
        use std::os::android::net::SocketAddrExt;
        #[cfg(target_os = "linux")]
        use std::os::linux::net::SocketAddrExt;
        let name = format!("rns/{instance_name}");
        std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())
            .and_then(|addr| std::os::unix::net::UnixStream::connect_addr(&addr))
            .is_ok()
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = instance_name;
        false
    }
}

async fn start_reticulum(settings: &Settings, shutdown: ShutdownSignal) -> Result<ReticulumHandle, String> {
    let dir = settings.rns_config_dir();
    if let Some(config) = settings.generated_rns_config() {
        std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        write_atomic(&dir.join("config"), config.as_bytes()).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    if let RnsAccess::Shared { instance_name } = &settings.rns
        && !shared_instance_listening(instance_name)
    {
        return Err(format!(
            "no Reticulum shared instance {instance_name:?} is running on this host (start rnsd first)"
        ));
    }
    let dir = dir.to_string_lossy().to_string();
    let runtime = init(Some(&dir), None, shutdown, Arc::new(AtomicBool::new(true)))
        .await
        .map_err(|e| format!("Reticulum did not start: {e}"))?;
    for (name, reason) in runtime.startup_interface_failures() {
        tracing::warn!("interface {name} failed to start: {reason}");
    }
    Ok(runtime)
}

/// Announce each local hub. The app data is the hub's default, which names
/// the upstream hub once its WELCOME has come.
async fn announce(handles: &[DestinationHandle]) {
    for handle in handles {
        match handle.announce(DestinationAnnounceOptions::default()).await {
            Ok(()) => tracing::info!("announced hub {}", hex::encode(handle.destination_hash())),
            Err(e) => tracing::warn!("could not announce hub {}: {e}", hex::encode(handle.destination_hash())),
        }
    }
}

async fn announcer(handles: Vec<DestinationHandle>, interval: Duration) {
    tokio::time::sleep(FIRST_ANNOUNCE_DELAY).await;
    loop {
        announce(&handles).await;
        tokio::time::sleep(interval).await;
    }
}

/// Tell the rnsd the bouncer connects to (`tcp` mode) where the local hubs
/// are, with a path response for each hub on the uplink. A path response is
/// not passed on by transport nodes, so it reaches that rnsd and nothing
/// beyond; the rnsd then answers path requests for the hubs from its own path
/// table, which it does not do for destinations it has never heard of.
///
/// Sent on each interface that has come online since the last check and,
/// with `refresh`, on every online interface. The refresh keeps rnsd's path
/// from being dropped as never used, and covers an rnsd restart or reconnect
/// that happened between two checks. Each path response is built afresh (no
/// tag, so rsReticulum does not reuse a cached one): a new random blob with
/// the current time in it, which rnsd takes as a more recent emission and
/// uses to replace its path entry (`Transport.py:2520-2523`).
async fn uplink_path_responses(
    runtime: &ReticulumHandle,
    hubs: &[DestinationHandle],
    online: &mut HashSet<u64>,
    refresh: bool,
) {
    let stats = match runtime.interface_stats().await {
        Ok(stats) => stats,
        Err(e) => {
            tracing::debug!("interface stats: {e}");
            return;
        }
    };
    let now_online: HashSet<u64> = stats.interfaces.iter().filter(|i| i.online).map(|i| i.id).collect();
    let targets: Vec<u64> = if refresh {
        now_online.iter().copied().collect()
    } else {
        now_online.difference(online).copied().collect()
    };
    for interface in targets {
        for handle in hubs {
            let options = DestinationAnnounceOptions {
                path_response: true,
                attached_interface: Some(interface),
                ..DestinationAnnounceOptions::default()
            };
            match handle.announce(options).await {
                Ok(()) => tracing::info!(
                    "sent the uplink a path response for hub {}{}",
                    hex::encode(handle.destination_hash()),
                    if refresh { " (refresh)" } else { "" }
                ),
                Err(e) => tracing::warn!(
                    "could not send a path response for hub {}: {e}",
                    hex::encode(handle.destination_hash())
                ),
            }
        }
    }
    *online = now_online;
}

/// Writes status.json off the bouncer's task, one write at a time and only
/// the latest status: a slow disk (an fsync can take seconds) never holds
/// up the run loop, or the signal that stops it.
struct StatusWriter {
    tx: watch::Sender<Option<Vec<u8>>>,
    task: tokio::task::JoinHandle<()>,
}

impl StatusWriter {
    fn start(path: std::path::PathBuf) -> Self {
        // One slot: a snapshot replaces the one not written yet.
        let (tx, mut rx) = watch::channel::<Option<Vec<u8>>>(None);
        let task = tokio::spawn(async move {
            // changed() still yields a value set before the sender went.
            while rx.changed().await.is_ok() {
                let Some(data) = rx.borrow_and_update().clone() else {
                    continue;
                };
                let path = path.clone();
                let written = tokio::task::spawn_blocking(move || {
                    write_atomic(&path, &data).map_err(|e| format!("{}: {e}", path.display()))
                })
                .await;
                match written {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::warn!("{e}"),
                    Err(e) => tracing::warn!("status: {e}"),
                }
            }
        });
        Self { tx, task }
    }

    fn write(&self, status: &Status) {
        match serde_json::to_vec_pretty(status) {
            Ok(data) => {
                self.tx.send_replace(Some(data));
            }
            Err(e) => tracing::warn!("status: {e}"),
        }
    }

    /// Write `status` last and wait for it, at most `within`: false if the
    /// write did not end in time.
    async fn stop(self, status: &Status, within: Duration) -> bool {
        self.write(status);
        let Self { tx, task } = self;
        drop(tx);
        tokio::time::timeout(within, task).await.is_ok()
    }
}

fn show_sessions(status: &mut Status, sessions: &[(SharedStatus, SharedRelayStatus, SharedHistoryStatus)]) {
    for (hub, (session, relay, history)) in status.hubs.iter_mut().zip(sessions) {
        if let Ok(link) = session.lock() {
            hub.link = link.clone();
        }
        if let Ok(relay) = relay.lock() {
            hub.relay = relay.clone();
        }
        if let Ok(history) = history.lock() {
            hub.history = history.clone();
        }
    }
    status.updated_at = now();
}

pub async fn run(settings: Settings) -> Result<(), String> {
    let identity = Arc::new(load_identity(&settings.identity)?);
    create_private_dir(&settings.state_dir)?;
    // Held until the bouncer exits: a second one on the same state_dir
    // would write the same history and status.
    let _lock = lock_state_dir(&settings.state_dir)?;
    let mut locals = Vec::new();
    for hub in &settings.hubs {
        let local = load_or_create_hub_identity(&settings.hub_identity_path(&hub.address), &identity)?;
        let rooms = RoomSet::load(&settings.rooms_path(&hub.address), &hub.rooms)?;
        let history_status = SharedHistoryStatus::default();
        let history = History::start(
            settings.history_dir(&hub.address),
            settings.history.clone(),
            history_status.clone(),
        )?;
        locals.push((hub, local, rooms, history, history_status));
    }

    // A signal stops the bouncer, not Reticulum: the upstream sessions close
    // their links first, while the transport can still send the close and
    // clean the link up, and `shutdown_and_wait` stops Reticulum after.
    let mut signals = rns_runtime::lifecycle::install_signal_handlers(ShutdownSignal::new());
    let runtime = start_reticulum(&settings, ShutdownSignal::new()).await?;

    let mut allowed = vec![identity.hash];
    allowed.extend(settings.allowed_identities.iter().copied());
    let timings = Timings::from_env();
    let changed = Arc::new(Notify::new());
    let renamed = Arc::new(Notify::new());
    let (stop_sessions, sessions_stop) = watch::channel(false);
    let mut tasks = Vec::new();
    let mut sessions = Vec::new();
    let mut session_status = Vec::new();
    let mut announced = Vec::new();
    let mut status = Status {
        pid: std::process::id(),
        started_at: now(),
        updated_at: now(),
        identity_hash: hex::encode(identity.hash),
        announce: settings.announce,
        hubs: Vec::new(),
    };
    let mut histories = Vec::new();
    for (hub, local, rooms, history, history_status) in locals {
        let upstream = hub.address;
        let hub_identity = local.hash;
        let identities = LinkIdentities::default();
        let slots = relay::ResourceSlots::default();
        let relay_status = SharedRelayStatus::default();
        let (identified_tx, identified_rx) = mpsc::unbounded_channel();
        let registered = runtime
            .register_destination(
                local,
                HUB_ASPECT,
                DestinationRuntimeOptions {
                    proof_strategy: ProofStrategy::ProveNone,
                    resource_strategy: ResourceStrategy::AcceptApp,
                    resource_accept: Some(relay::resource_policy(
                        identities.clone(),
                        allowed.clone(),
                        slots.clone(),
                        relay_status.clone(),
                    )),
                    // Room for the Resource events of every link at once.
                    event_capacity: HUB_EVENTS,
                    default_app_data: Some(hub_app_data(&local_hub_name(&upstream))),
                    identity_gate: Some(relay::identity_gate(identities.clone(), identified_tx)),
                    ..DestinationRuntimeOptions::default()
                },
            )
            .await
            .map_err(|e| format!("could not register the local hub for {}: {e}", hex::encode(upstream)))?;
        let address = registered.handle.destination_hash();
        tracing::info!(
            "local hub {} for upstream hub {}",
            hex::encode(address),
            hex::encode(upstream)
        );
        status.hubs.push(HubStatus {
            upstream: hex::encode(upstream),
            local_hub: hex::encode(address),
            link: UpstreamStatus::default(),
            relay: RelayStatus::default(),
            history: HistoryStatus::default(),
        });
        announced.push(registered.handle.clone());
        let shared = SharedStatus::default();
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE);
        let (event_tx, event_rx) = upstream::event_channel();
        let relay = Relay::new(RelayConfig {
            hub_identity,
            user: identity.hash,
            upstream,
            allowed: allowed.clone(),
            part_upstream: settings.part_upstream,
            nick: hub.nick.clone(),
            commands: command_tx,
            session: shared.clone(),
            status: relay_status.clone(),
            history: history.clone(),
            replay_window: settings.history.replay_window,
            replay_max: settings.history.replay_max,
        });
        histories.push(history);
        sessions.push(tokio::spawn(upstream::run(
            Upstream {
                runtime: runtime.clone(),
                address: upstream,
                identity: identity.clone(),
                nick: hub.nick.clone(),
                rooms,
                local_hub: registered.handle.clone(),
                timings,
                status: shared.clone(),
                changed: changed.clone(),
                renamed: renamed.clone(),
                events: Some(event_tx),
            },
            command_rx,
            sessions_stop.clone(),
        )));
        session_status.push((shared, relay_status, history_status));
        tasks.push(tokio::spawn(relay::run(LocalHub {
            registered,
            identities,
            identified: identified_rx,
            events: event_rx,
            relay,
            changed: changed.clone(),
            slots,
        })));
    }
    let uplink = !settings.announce && matches!(settings.rns, RnsAccess::Tcp { .. });
    if settings.announce {
        tasks.push(tokio::spawn(announcer(announced.clone(), settings.announce_interval)));
    } else {
        tracing::info!("not announcing the local hubs (announce = false); they answer path requests");
    }

    let status_writer = StatusWriter::start(settings.status_path());
    status_writer.write(&status);
    let mut ticker = tokio::time::interval(STATUS_INTERVAL);
    ticker.tick().await;
    let mut uplink_check = tokio::time::interval(UPLINK_CHECK);
    let mut uplink_refresh = tokio::time::interval(uplink_refresh());
    // The first refresh is due one interval after start; the online edge
    // covers the start.
    uplink_refresh.reset();
    let mut online = HashSet::new();
    loop {
        tokio::select! {
            _ = signals.recv() => break,
            _ = ticker.tick() => {
                show_sessions(&mut status, &session_status);
                status_writer.write(&status);
            }
            _ = changed.notified() => {
                show_sessions(&mut status, &session_status);
                status_writer.write(&status);
            }
            // A local hub took its upstream hub's name: tell the network
            // now rather than at the next announce or refresh, in a later
            // second than the announce or path response with the old name.
            _ = renamed.notified() => {
                tokio::time::sleep(until_next_second(now())).await;
                if settings.announce {
                    announce(&announced).await;
                } else if uplink {
                    uplink_path_responses(&runtime, &announced, &mut online, true).await;
                }
            }
            _ = uplink_check.tick(), if uplink => {
                uplink_path_responses(&runtime, &announced, &mut online, false).await;
            }
            _ = uplink_refresh.tick(), if uplink => {
                uplink_path_responses(&runtime, &announced, &mut online, true).await;
            }
        }
    }
    tracing::info!("shutting down");
    // The sessions close their links, so the hubs see the user leave now
    // rather than at their ping timeout.
    let _ = stop_sessions.send(true);
    let stopping = futures_join_all(sessions);
    if tokio::time::timeout(SESSION_STOP, stopping).await.is_err() {
        tracing::warn!("upstream sessions did not stop in {}s", SESSION_STOP.as_secs());
    }
    for task in &tasks {
        task.abort();
    }
    // What the history threads were given is written and synced.
    // All hubs at once, each within HISTORY_STOP.
    let stopping: Vec<_> = histories
        .iter()
        .map(|history| {
            let history = history.clone();
            tokio::spawn(async move { history.stop(HISTORY_STOP).await })
        })
        .collect();
    for stop in stopping {
        if !stop.await.unwrap_or(false) {
            tracing::warn!("the history did not stop in {}s", HISTORY_STOP.as_secs());
        }
    }
    show_sessions(&mut status, &session_status);
    if !status_writer.stop(&status, STATUS_STOP).await {
        tracing::warn!("status.json was not written in {}s", STATUS_STOP.as_secs());
    }
    if tokio::time::timeout(RETICULUM_STOP, runtime.shutdown_and_wait())
        .await
        .is_err()
    {
        tracing::warn!("Reticulum did not stop in {}s", RETICULUM_STOP.as_secs());
    }
    Ok(())
}

/// Take the lock on `state_dir/lock`, or say another bouncer holds it.
pub fn lock_state_dir(state_dir: &std::path::Path) -> Result<std::fs::File, String> {
    let path = state_dir.join(LOCK_FILE);
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let file = options.open(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(format!(
            "another rrc-bouncer is running with state_dir {} (it holds {})",
            state_dir.display(),
            path.display()
        )),
        Err(std::fs::TryLockError::Error(e)) => Err(format!("{}: {e}", path.display())),
    }
}

/// Wait for every task (aborting the rest if the caller times out).
async fn futures_join_all(tasks: Vec<tokio::task::JoinHandle<()>>) {
    struct AbortOnDrop(Vec<tokio::task::AbortHandle>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            for task in &self.0 {
                task.abort();
            }
        }
    }
    let _guard = AbortOnDrop(tasks.iter().map(tokio::task::JoinHandle::abort_handle).collect());
    for task in tasks {
        let _ = task.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_status_writer_writes_the_latest_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("status.json");
        let writer = StatusWriter::start(path.clone());
        let status = |n: u32| Status {
            pid: n,
            started_at: 0.0,
            updated_at: f64::from(n),
            identity_hash: String::new(),
            announce: false,
            hubs: Vec::new(),
        };
        // Snapshots come faster than they are written: each replaces the
        // one not written yet, and the last is what stays.
        for n in 0..1000 {
            writer.write(&status(n));
        }
        assert!(writer.stop(&status(1000), Duration::from_secs(10)).await);
        let read: Status = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(read.pid, 1000);
    }

    #[test]
    fn every_status_field_has_a_key_of_its_own() {
        // The parts are flattened into one object: a name in two of them
        // would be written twice and read back into one.
        let hub = HubStatus {
            upstream: "u".into(),
            local_hub: "l".into(),
            link: UpstreamStatus {
                hub_resources_in: 1,
                hub_resource_drops: 3,
                ..Default::default()
            },
            relay: RelayStatus {
                client_resources_in: 4,
                client_resources_out: 5,
                client_resources_split: 6,
                client_resource_drops: 7,
                ..Default::default()
            },
            history: HistoryStatus::default(),
        };
        let value = serde_json::to_value(&hub).unwrap();
        let keys = |v: serde_json::Value| v.as_object().unwrap().keys().cloned().collect::<Vec<_>>();
        let mut all = keys(serde_json::to_value(&hub.link).unwrap());
        all.extend(keys(serde_json::to_value(&hub.relay).unwrap()));
        all.extend(keys(serde_json::to_value(&hub.history).unwrap()));
        let parts = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), parts);
        let back: HubStatus = serde_json::from_value(value).unwrap();
        assert_eq!((back.link, back.relay), (hub.link, hub.relay));
    }

    #[test]
    fn the_rename_waits_for_the_next_whole_second() {
        assert_eq!(until_next_second(100.25), Duration::from_millis(760));
        assert_eq!(until_next_second(100.0), Duration::from_millis(1010));
        let wait = until_next_second(100.999_999);
        assert!(wait > Duration::from_millis(10) && wait < Duration::from_millis(11));
        // Whatever was sent before the wait has an earlier whole second.
        for now in [5.0, 5.5, 5.999] {
            let after = now + until_next_second(now).as_secs_f64();
            assert!(after.floor() > f64::floor(now));
        }
    }

    #[test]
    fn the_lock_file_keeps_a_second_bouncer_out() {
        let dir = tempfile::tempdir().unwrap();
        let first = lock_state_dir(dir.path()).unwrap();
        let err = lock_state_dir(dir.path()).unwrap_err();
        assert!(err.contains("another rrc-bouncer is running"), "{err}");
        // status reads the state_dir all the same.
        assert!(dir.path().join(LOCK_FILE).exists());
        drop(first);
        lock_state_dir(dir.path()).unwrap();
    }

    #[tokio::test]
    async fn the_status_writer_writes_the_last_status_at_stop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("status.json");
        let status = |pid| Status {
            pid,
            started_at: 0.0,
            updated_at: 0.0,
            identity_hash: String::new(),
            announce: false,
            hubs: Vec::new(),
        };
        let writer = StatusWriter::start(path.clone());
        for pid in 1..=50 {
            writer.write(&status(pid));
        }
        assert!(writer.stop(&status(99), STATUS_STOP).await);
        let written: Status = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(written.pid, 99);
        assert!(!path.with_extension("tmp").exists());
    }

    #[test]
    fn a_long_upstream_name_is_cut_to_fit_the_announce() {
        let long = "é".repeat(200);
        let name = upstream::local_name(&long);
        assert!(name.len() <= upstream::MAX_HUB_NAME_BYTES, "{}", name.len());
        assert!(name.ends_with(" (via rrc-bouncer)"));
        assert!(name.starts_with("éé"));
        assert!(hub_app_data(&name).len() <= 120);
        // An odd byte budget lands inside a two-byte character.
        let mixed = format!("a{}", "é".repeat(100));
        let name = upstream::local_name(&mixed);
        assert!(name.len() <= upstream::MAX_HUB_NAME_BYTES);
        assert_eq!(name.len(), upstream::MAX_HUB_NAME_BYTES - 1);
        assert_eq!(upstream::local_name("test hub"), "test hub (via rrc-bouncer)");
    }

    #[test]
    fn app_data_is_what_rrcd_announces() {
        // cbor2.dumps({"proto": "rrc", "v": 1, "hub": "test hub"})
        assert_eq!(
            hex::encode(hub_app_data("test hub")),
            "a36570726f746f6372726361760163687562687465737420687562"
        );
    }
}
