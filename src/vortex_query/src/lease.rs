//! Private ownership of a verified immutable index generation.
use crate::{LIMITS, provider};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use vortex::error::{VortexResult, vortex_bail, vortex_err};
use vortex_index::store::{LocalGeneration, LocalIndexStore};
use vortex_index::{Index, IndexProvider, Snapshot};
pub struct ViewBudget {
    pub source_bytes: usize,
    pub artifact_bytes: u64,
    pub inline_rows: usize,
    pub inline_bytes: usize,
}

pub struct IndexLease {
    // Close native handles before releasing their private directories.
    pub index: Arc<dyn Index>,
    _scratch: TempDir,
    pub artifact_bytes: u64,
}

impl IndexLease {
    pub async fn open(
        reference: &Path,
        generation: &LocalGeneration,
        snapshot: &Snapshot,
        leases: &Path,
        artifact_budget: u64,
    ) -> VortexResult<Self> {
        let root = reference
            .parent()
            .ok_or_else(|| vortex_err!("Reference parent required"))?;
        Self::open_root(root, generation, snapshot, leases, artifact_budget).await
    }

    pub async fn open_root(
        root: &Path,
        generation: &LocalGeneration,
        snapshot: &Snapshot,
        leases: &Path,
        artifact_budget: u64,
    ) -> VortexResult<Self> {
        let pending = LocalIndexStore::prepare_open(root, generation, snapshot, LIMITS)?;
        let artifact_bytes =
            pending
                .metadata()
                .artifacts
                .iter()
                .try_fold(0u64, |total, artifact| {
                    total
                        .checked_add(artifact.size)
                        .ok_or_else(|| vortex_err!("Artifact byte count overflow"))
                })?;
        if artifact_bytes > artifact_budget {
            vortex_bail!("Retained artifact byte budget exceeded");
        }
        let scratch = tempfile::Builder::new()
            .prefix(".read-view-")
            .tempdir_in(leases)?;
        let (store, metadata) = pending.materialize(scratch.path(), artifact_budget)?;
        let index = provider(scratch.path())?.open(&metadata, store).await?;
        Ok(Self {
            index,
            _scratch: scratch,
            artifact_bytes,
        })
    }
}
