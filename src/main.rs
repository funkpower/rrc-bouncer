// SPDX-License-Identifier: LicenseRef-Reticulum
//! The `rrc-bouncer` command.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rrc_bouncer::{bouncer, config};

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
    }
    Ok(())
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
                Ok(runtime) => runtime.block_on(bouncer::run(settings)),
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
