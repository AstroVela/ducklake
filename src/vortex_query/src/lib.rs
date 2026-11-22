//! Bounded DuckLake physical-source and SPFresh B/F/T query library.
pub mod admission;
mod build;
mod ffi;
pub mod file_index;
pub mod lease;
pub mod mixed;
pub mod source;
pub use build::{build_generation, write_reference};
use std::path::Path;
use vortex::error::VortexResult;
use vortex_index::store::LocalStoreLimits;
use vortex_index_spfresh::{SpFreshLimits, SpFreshProvider};
pub const LIMITS: LocalStoreLimits = LocalStoreLimits {
    max_artifact_bytes: 64 * 1024 * 1024,
    max_manifest_bytes: 1024 * 1024,
};

pub fn provider(scratch: &Path) -> VortexResult<SpFreshProvider> {
    SpFreshProvider::try_new(scratch.to_path_buf(), SpFreshLimits::default())
}
