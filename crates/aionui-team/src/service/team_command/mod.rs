#![cfg_attr(not(test), allow(dead_code))]

mod actor;
mod dispatch;
mod model;
mod service;
mod snapshot;

#[allow(unused_imports)]
pub(crate) use model::{
    ConflictedIntegrationEvidence, MergedIntegrationEvidence, RetryableIntegrationEvidence, TeamCommand,
    TeamCommandDeliveryResult, TeamCommandError, TeamCommandPrincipal, TeamCommandReceipt, TeamCommandResult,
};
pub use service::TeamCommandService;

#[cfg(test)]
mod tests;
