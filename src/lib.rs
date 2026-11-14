// SPDX-License-Identifier: LicenseRef-Reticulum
//! rrc-bouncer: hold one RRC identity for several RRC clients.
//!
//! The bouncer keeps one link to each upstream hub, identified as the user,
//! and stays in the user's rooms. Clients link to a hub destination the
//! bouncer serves for each upstream hub and speak plain RRC to it; the
//! bouncer relays both ways and replays the history it kept when a client
//! joins a room. See DESIGN.md.

pub mod bouncer;
pub mod config;
pub mod envelope;
pub mod history;
pub mod identity;
pub mod rate;
pub mod relay;
pub mod rooms;
pub mod upstream;
