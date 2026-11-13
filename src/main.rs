// SPDX-License-Identifier: LicenseRef-Reticulum
//! rrc-bouncer: hold one RRC identity for several RRC clients.
//!
//! The bouncer keeps one link to each upstream hub, identified as the user,
//! and stays in the user's rooms. Clients link to a hub destination the
//! bouncer serves for each upstream hub and speak plain RRC to it; the
//! bouncer relays both ways and replays the history it kept when a client
//! joins a room. See DESIGN.md.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about = "Hold one RRC identity for several RRC clients, like an IRC bouncer")]
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

fn main() -> ExitCode {
    let cli = Cli::parse();
    let _ = cli.config;
    match cli.command.unwrap_or(Command::Run) {
        Command::Run | Command::Status { .. } | Command::Hashes => {
            eprintln!("rrc-bouncer: not implemented yet");
            ExitCode::FAILURE
        }
    }
}
