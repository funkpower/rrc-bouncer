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
use crate::identity::{HUB_ASPECT, create_private_dir, hub_address, load_identity, load_or_create_hub_identity};
use crate::relay::{self, LinkIdentities, LocalHub, Relay, RelayConfig, RelayStatus, SharedRelayStatus};
use crate::rooms::RoomSet;
use crate::upstream::{self, COMMAND_QUEUE, EVENT_QUEUE, SharedStatus, Timings, Upstream, UpstreamStatus};

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
/// How long the upstream sessions get to close their links at shutdown:
/// longer than a session waits for its link to close, so each one gets to
/// write `stopped` to its status.
const SESSION_STOP: Duration = Duration::from_secs(10);
const _: () = assert!(SESSION_STOP.as_secs() > upstream::CLOSE_WAIT.as_secs());

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

fn write_status(settings: &Settings, status: &Status) {
    let path = settings.status_path();
    match serde_json::to_vec_pretty(status) {
        Ok(data) => {
            if let Err(e) = write_atomic(&path, &data) {
                tracing::warn!("{}: {e}", path.display());
            }
        }
        Err(e) => tracing::warn!("status: {e}"),
    }
}

fn show_sessions(status: &mut Status, sessions: &[(SharedStatus, SharedRelayStatus)]) {
    for (hub, (session, relay)) in status.hubs.iter_mut().zip(sessions) {
        if let Ok(link) = session.lock() {
            hub.link = link.clone();
        }
        if let Ok(relay) = relay.lock() {
            hub.relay = relay.clone();
        }
    }
    status.updated_at = now();
}

pub async fn run(settings: Settings) -> Result<(), String> {
    let identity = Arc::new(load_identity(&settings.identity)?);
    create_private_dir(&settings.state_dir)?;
    let mut locals = Vec::new();
    for hub in &settings.hubs {
        let local = load_or_create_hub_identity(&settings.hub_identity_path(&hub.address), &identity)?;
        let rooms = RoomSet::load(&settings.rooms_path(&hub.address), &hub.rooms)?;
        locals.push((hub, local, rooms));
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
    for (hub, local, rooms) in locals {
        let upstream = hub.address;
        let hub_identity = local.hash;
        let identities = LinkIdentities::default();
        let (identified_tx, identified_rx) = mpsc::unbounded_channel();
        let registered = runtime
            .register_destination(
                local,
                HUB_ASPECT,
                DestinationRuntimeOptions {
                    proof_strategy: ProofStrategy::ProveNone,
                    resource_strategy: ResourceStrategy::AcceptNone,
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
        });
        announced.push(registered.handle.clone());
        let shared = SharedStatus::default();
        let relay_status = SharedRelayStatus::default();
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE);
        let (event_tx, event_rx) = mpsc::channel(EVENT_QUEUE);
        let relay = Relay::new(RelayConfig {
            hub_identity,
            user: identity.hash,
            upstream,
            allowed: allowed.clone(),
            part_upstream: settings.part_upstream,
            commands: command_tx,
            session: shared.clone(),
            status: relay_status.clone(),
        });
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
        session_status.push((shared, relay_status));
        tasks.push(tokio::spawn(relay::run(LocalHub {
            registered,
            identities,
            identified: identified_rx,
            events: event_rx,
            relay,
            changed: changed.clone(),
        })));
    }
    let uplink = !settings.announce && matches!(settings.rns, RnsAccess::Tcp { .. });
    if settings.announce {
        tasks.push(tokio::spawn(announcer(announced.clone(), settings.announce_interval)));
    } else {
        tracing::info!("not announcing the local hubs (announce = false); they answer path requests");
    }

    write_status(&settings, &status);
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
                write_status(&settings, &status);
            }
            _ = changed.notified() => {
                show_sessions(&mut status, &session_status);
                write_status(&settings, &status);
            }
            // A local hub took its upstream hub's name: tell the network
            // now rather than at the next announce or refresh.
            _ = renamed.notified() => {
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
    show_sessions(&mut status, &session_status);
    write_status(&settings, &status);
    runtime.shutdown_and_wait().await;
    Ok(())
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
