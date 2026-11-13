// SPDX-License-Identifier: LicenseRef-Reticulum
//! Starting Reticulum and running the local hubs.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use ciborium::Value;
use rns_runtime::prelude::*;
use serde::{Deserialize, Serialize};

use crate::config::{RnsAccess, Settings};
use crate::envelope::{Envelope, MessageType};
use crate::identity::{HUB_ASPECT, create_private_dir, hub_address, load_identity, load_or_create_hub_identity};

/// How long to wait after starting before the first announce, so
/// interfaces have come up.
const FIRST_ANNOUNCE_DELAY: Duration = Duration::from_secs(5);
const STATUS_INTERVAL: Duration = Duration::from_secs(60);
/// How often the uplink is checked for having (re)connected.
const UPLINK_CHECK: Duration = Duration::from_secs(10);

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

/// A local hub's link events. Until the relay exists it only logs links
/// and answers PING from the user's own clients.
async fn local_hub_events(mut hub: RegisteredDestination, hub_identity: [u8; 16], allowed: Vec<[u8; 16]>) {
    let tag = hex::encode(hub.handle.destination_hash());
    let mut identified: Vec<([u8; 16], [u8; 16])> = Vec::new();
    let events = &mut hub.events;
    loop {
        tokio::select! {
            Some(link) = events.links_established.recv() => {
                tracing::info!("hub {tag}: link {} established", hex::encode(link));
            }
            Some((link, identity)) = events.links_identified.recv() => {
                let known = allowed.contains(&identity);
                tracing::info!(
                    "hub {tag}: link {} identified as {}{}",
                    hex::encode(link),
                    hex::encode(identity),
                    if known { "" } else { " (not allowed)" }
                );
                if known {
                    identified.push((link, identity));
                }
            }
            Some((data, link)) = events.link_packets.recv() => {
                if !identified.iter().any(|(l, _)| *l == link) {
                    tracing::debug!("hub {tag}: ignored a packet on a link not identified as an allowed identity");
                    continue;
                }
                match Envelope::decode(&data) {
                    Ok(env) if env.t == MessageType::Ping => {
                        // The hub's own envelopes carry its identity in K_SRC.
                        let mut pong = Envelope::new(MessageType::Pong, &hub_identity);
                        pong.body = env.body;
                        if let Err(e) = hub.handle.send_link_packet(link, pong.encode()).await {
                            tracing::warn!("hub {tag}: could not answer PING: {e}");
                        }
                    }
                    Ok(env) => tracing::debug!("hub {tag}: {:?} not handled yet", env.t),
                    Err(e) => tracing::debug!("hub {tag}: {e}"),
                }
            }
            Some(link) = events.links_closed.recv() => {
                identified.retain(|(l, _)| *l != link);
                tracing::info!("hub {tag}: link {} closed", hex::encode(link));
            }
            Some(_) = events.packets.recv() => {}
            Some(_) = events.link_packet_proofs.recv() => {}
            Some(_) = events.resource_completions.recv() => {}
            Some(_) = events.resource_proofs.recv() => {}
            Some(_) = events.resource_events.recv() => {}
            Some(_) = events.channel_messages.recv() => {}
            else => break,
        }
    }
}

async fn announcer(handles: Vec<(DestinationHandle, Vec<u8>)>, interval: Duration) {
    tokio::time::sleep(FIRST_ANNOUNCE_DELAY).await;
    loop {
        for (handle, app_data) in &handles {
            let options = DestinationAnnounceOptions {
                app_data: Some(app_data.clone()),
                ..DestinationAnnounceOptions::default()
            };
            match handle.announce(options).await {
                Ok(()) => tracing::info!("announced hub {}", hex::encode(handle.destination_hash())),
                Err(e) => tracing::warn!("could not announce hub {}: {e}", hex::encode(handle.destination_hash())),
            }
        }
        tokio::time::sleep(interval).await;
    }
}

/// Tell the rnsd the bouncer connects to (`tcp` mode) where the local hubs
/// are, with a path response on each interface that has just come online. A
/// path response is not passed on by transport nodes, so it reaches that
/// rnsd and nothing beyond; the rnsd then answers path requests for the hubs
/// from its own path table, which it does not do for destinations it has
/// never heard of.
async fn uplink_path_responses(
    runtime: &ReticulumHandle,
    hubs: &[(DestinationHandle, Vec<u8>)],
    online: &mut std::collections::HashSet<u64>,
) {
    let stats = match runtime.interface_stats().await {
        Ok(stats) => stats,
        Err(e) => {
            tracing::debug!("interface stats: {e}");
            return;
        }
    };
    let now_online: std::collections::HashSet<u64> =
        stats.interfaces.iter().filter(|i| i.online).map(|i| i.id).collect();
    for interface in now_online.difference(online) {
        for (handle, app_data) in hubs {
            let options = DestinationAnnounceOptions {
                app_data: Some(app_data.clone()),
                path_response: true,
                attached_interface: Some(*interface),
                ..DestinationAnnounceOptions::default()
            };
            match handle.announce(options).await {
                Ok(()) => tracing::info!(
                    "sent the uplink a path response for hub {}",
                    hex::encode(handle.destination_hash())
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

pub async fn run(settings: Settings) -> Result<(), String> {
    let identity = load_identity(&settings.identity)?;
    create_private_dir(&settings.state_dir)?;
    let mut locals = Vec::new();
    for hub in &settings.hubs {
        let local = load_or_create_hub_identity(&settings.hub_identity_path(&hub.address), &identity)?;
        locals.push((hub.address, local));
    }

    let shutdown = ShutdownSignal::new();
    let mut signals = rns_runtime::lifecycle::install_signal_handlers(shutdown.clone());
    let runtime = start_reticulum(&settings, shutdown.clone()).await?;

    let mut allowed = vec![identity.hash];
    allowed.extend(settings.allowed_identities.iter().copied());
    let mut tasks = Vec::new();
    let mut announced = Vec::new();
    let mut status = Status {
        pid: std::process::id(),
        started_at: now(),
        updated_at: now(),
        identity_hash: hex::encode(identity.hash),
        announce: settings.announce,
        hubs: Vec::new(),
    };
    for (upstream, local) in locals {
        let hub_identity = local.hash;
        let app_data = hub_app_data(&local_hub_name(&upstream));
        let registered = runtime
            .register_destination(
                local,
                HUB_ASPECT,
                DestinationRuntimeOptions {
                    proof_strategy: ProofStrategy::ProveNone,
                    resource_strategy: ResourceStrategy::AcceptNone,
                    default_app_data: Some(app_data.clone()),
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
        });
        announced.push((registered.handle.clone(), app_data));
        tasks.push(tokio::spawn(local_hub_events(
            registered,
            hub_identity,
            allowed.clone(),
        )));
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
    let mut online = std::collections::HashSet::new();
    loop {
        tokio::select! {
            _ = signals.recv() => break,
            _ = ticker.tick() => {
                status.updated_at = now();
                write_status(&settings, &status);
            }
            _ = uplink_check.tick(), if uplink => {
                uplink_path_responses(&runtime, &announced, &mut online).await;
            }
        }
    }
    tracing::info!("shutting down");
    for task in &tasks {
        task.abort();
    }
    status.updated_at = now();
    write_status(&settings, &status);
    runtime.shutdown_and_wait().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_data_is_what_rrcd_announces() {
        // cbor2.dumps({"proto": "rrc", "v": 1, "hub": "test hub"})
        assert_eq!(
            hex::encode(hub_app_data("test hub")),
            "a36570726f746f6372726361760163687562687465737420687562"
        );
    }
}
