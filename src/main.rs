// SPDX-License-Identifier: LicenseRef-Reticulum
//! The `rrc-bouncer` command.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use rrc_bouncer::{bouncer, config};

/// How long tasks still running at exit get to stop.
const RUNTIME_STOP: Duration = Duration::from_secs(2);

#[derive(Parser)]
#[command(
    version,
    about = "Hold one RRC identity for several RRC clients, like an IRC bouncer"
)]
struct Cli {
    /// The configuration file.
    #[arg(
        short,
        long,
        global = true,
        default_value = "/etc/rrc-bouncer/config.toml",
        env = "RRC_BOUNCER_CONFIG"
    )]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the bouncer (the default).
    Run,
    /// Summarise what the running (or last run) bouncer holds.
    Status {
        /// Print the status file as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Print the hub address the bouncer serves for each upstream hub, to
    /// set in the clients.
    Hashes,
}

fn log_filter(level: &str) -> String {
    match level {
        "error" | "warn" | "info" => {
            format!("{level},rns_runtime=warn,rns_transport=warn,rns_link=warn,rns_interface=warn,rns_identity=warn")
        }
        other => other.to_string(),
    }
}

fn setup_logging(level: &str) {
    use tracing_subscriber::EnvFilter;
    let filter = std::env::var("RUST_LOG")
        .ok()
        .map_or_else(|| EnvFilter::new(log_filter(level)), EnvFilter::new);
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false);
    // journald stamps each line itself.
    if std::env::var_os("JOURNAL_STREAM").is_some() {
        builder.without_time().with_ansi(false).init();
    } else {
        builder.init();
    }
}

fn ago(at: f64) -> String {
    let seconds = (bouncer::now() - at).max(0.0) as u64;
    match seconds {
        s if s < 120 => format!("{s} s ago"),
        s if s < 7200 => format!("{} min ago", s / 60),
        s if s < 172_800 => format!("{} h ago", s / 3600),
        s => format!("{} days ago", s / 86_400),
    }
}

fn status(settings: &config::Settings, json: bool) -> Result<(), String> {
    let path = settings.status_path();
    let data = std::fs::read(&path).map_err(|e| format!("{}: {e} (has the bouncer run yet?)", path.display()))?;
    if json {
        println!("{}", String::from_utf8_lossy(&data));
        return Ok(());
    }
    let s: bouncer::Status = serde_json::from_slice(&data).map_err(|e| format!("{}: {e}", path.display()))?;
    let running = std::path::Path::new(&format!("/proc/{}", s.pid)).exists() && bouncer::now() - s.updated_at < 180.0;
    println!(
        "rrc-bouncer status (updated {}{})",
        ago(s.updated_at),
        if running { "" } else { "; not running" }
    );
    println!("  identity           {}", s.identity_hash);
    println!("  started            {}", ago(s.started_at));
    println!("  announce           {}", if s.announce { "on" } else { "off" });
    for hub in &s.hubs {
        println!("  hub {}  local hub {}", hub.upstream, hub.local_hub);
        print_upstream(&hub.link, running);
        print_relay(&hub.relay);
        print_history(&hub.history);
    }
    Ok(())
}

fn print_relay(r: &rrc_bouncer::relay::RelayStatus) {
    if r.client_identities.is_empty() {
        println!("    clients          none");
    } else {
        println!(
            "    clients          {} ({})",
            r.clients,
            r.client_identities.join(", ")
        );
    }
    println!("    relayed          {} in, {} out", r.relayed_in, r.relayed_out);
    if r.relay_drops > 0 {
        println!("    relay drops      {}", r.relay_drops);
    }
    if r.refused_links > 0 {
        println!("    refused links    {}", r.refused_links);
    }
}

fn print_history(h: &rrc_bouncer::history::HistoryStatus) {
    let rooms = if h.history_rooms == 1 { "room" } else { "rooms" };
    println!(
        "    history          {} bytes in {} {rooms}",
        h.history_bytes, h.history_rooms
    );
    println!(
        "    history written  {} records, {} dropped",
        h.history_records, h.history_drops
    );
    if h.history_sync_errors > 0 {
        println!("    history syncs    {} failed", h.history_sync_errors);
    }
}

fn print_upstream(u: &rrc_bouncer::upstream::UpstreamStatus, running: bool) {
    let name = u.hub_name.as_deref().map_or_else(String::new, |n| format!(" {n:?}"));
    let version = u
        .hub_version
        .as_deref()
        .map_or_else(String::new, |v| format!(" (version {v})"));
    let state = match u.state.as_str() {
        "connected" => format!(
            "connected to{name}{version}{}",
            u.last_connected_at
                .map_or_else(String::new, |t| format!(", since {}", ago(t)))
        ),
        "" => "not started".to_string(),
        other => other.to_string(),
    };
    println!(
        "    upstream         {}",
        if running {
            state
        } else {
            format!("{state} (when it stopped)")
        }
    );
    let joined = u.rooms.iter().filter(|r| u.rooms_joined.contains(r)).count();
    println!(
        "    rooms            {} (joined {joined} of {})",
        if u.rooms.is_empty() {
            "none".to_string()
        } else {
            u.rooms.join(", ")
        },
        u.rooms.len()
    );
    let mut missing: Vec<String> = u.rooms_failed.iter().map(|(r, why)| format!("{r} ({why})")).collect();
    missing.extend(
        u.rooms_retrying
            .iter()
            .map(|(r, why)| format!("{r} (trying again: {why})")),
    );
    let limit = u.hub_limits.as_ref().map(|l| l.max_rooms_per_session);
    missing.extend(u.rooms_over_limit.iter().map(|r| match limit {
        Some(n) => format!("{r} (over the hub's limit of {n} rooms)"),
        None => format!("{r} (over the hub's room limit)"),
    }));
    if !missing.is_empty() {
        println!("    not joined       {}", missing.join("; "));
    }
    println!(
        "    connects         {} ({} reconnect{}), {} failed attempt{}",
        u.connects,
        u.reconnects,
        if u.reconnects == 1 { "" } else { "s" },
        u.failed_attempts,
        if u.failed_attempts == 1 { "" } else { "s" }
    );
    if let Some(at) = u.last_disconnected_at {
        let why = u.last_error.as_deref().map_or_else(String::new, |e| format!(": {e}"));
        println!("    last disconnect  {}{why}", ago(at));
    } else if let Some(e) = &u.last_error {
        println!("    last error       {e}");
    }
    if let Some(at) = u.next_attempt_at {
        println!(
            "    next attempt     in {} s",
            (at - bouncer::now()).max(0.0).round() as u64
        );
    }
    if !u.unhandled.is_empty() {
        let counts: Vec<String> = u.unhandled.iter().map(|(t, n)| format!("{t} {n}")).collect();
        println!("    not relayed yet  {}", counts.join(", "));
    }
    if u.dropped_sends > 0 {
        println!("    dropped sends    {}", u.dropped_sends);
    }
    if u.dropped_events > 0 {
        println!("    dropped events   {}", u.dropped_events);
    }
}

fn hashes(settings: &config::Settings) -> Result<(), String> {
    let (identity, hubs) = bouncer::hubs(settings)?;
    println!("identity           {identity}");
    for hub in hubs {
        println!("upstream hub       {}", hub.upstream);
        println!("  local hub        {}", hub.local_hub);
    }
    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let settings = match config::Settings::load(&cli.config) {
        Ok(settings) => settings,
        Err(e) => {
            eprintln!("rrc-bouncer: {e}");
            return ExitCode::from(2);
        }
    };
    let result = match cli.command.unwrap_or(Command::Run) {
        Command::Status { json } => status(&settings, json),
        Command::Hashes => hashes(&settings),
        Command::Run => {
            setup_logging(&settings.log_level);
            match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
                Ok(runtime) => {
                    // The bouncer runs on the runtime's workers and the main
                    // thread only waits for it. The kernel gives a signal sent
                    // to the process to its main thread first, and a thread in
                    // a blocking call (an fsync on a busy disk) does not take
                    // it until the call returns: so the main thread does none.
                    let result = runtime.block_on(async {
                        tokio::spawn(bouncer::run(settings))
                            .await
                            .unwrap_or_else(|e| Err(format!("the bouncer's task failed: {e}")))
                    });
                    // A task that does not stop does not hold the exit up.
                    runtime.shutdown_timeout(RUNTIME_STOP);
                    result
                }
                Err(e) => Err(format!("could not start the async runtime: {e}")),
            }
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e}");
            eprintln!("rrc-bouncer: {e}");
            ExitCode::FAILURE
        }
    }
}
