//! Shared library crate for Velo Search.
//!
//! This crate exists so that URL normalization has exactly one definition that
//! the service, the `eval` harness and any reindex tooling all link against.

pub mod urlutil;

pub use urlutil::canonical_url;