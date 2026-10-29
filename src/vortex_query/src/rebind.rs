//! Construct native bytes once, then bind their recorded row order to final file IDs.
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::{TryStreamExt, stream::BoxStream};
use vortex::{
    error::{VortexResult, vortex_bail, vortex_err},
    session::VortexSession,
};
use vortex_index::file::file_version;
use vortex_index::store::{LocalGeneration, LocalIndexStore};
use vortex_index::{
    IndexMetadata, IndexProvider, IndexSource, LocalIndexFiles, RowAddress, Snapshot, SourceBatch,
};
use vortex_index_spfresh::{SpFreshBundle, import_bundle};

use crate::{
    LIMITS,
    build::{BuildTarget, POSTING_PAGE_LIMIT, build_generation_at},
    file_index::{self, FileSource},
    provider,
    source::{Capture, PhysicalSource},
};

struct RecordedSource {
    source: FileSource,
    rows: Arc<Mutex<Vec<RowAddress>>>,
}

#[async_trait]
impl IndexSource for RecordedSource {
    fn snapshot(&self) -> &Snapshot {
        self.source.snapshot()
    }

    fn scan(
        &self,
        files: &[u64],
        fields: &[String],
    ) -> VortexResult<BoxStream<'static, VortexResult<SourceBatch>>> {
        let rows = Arc::clone(&self.rows);
        Ok(Box::pin(self.source.scan(files, fields)?.and_then(
            move |batch| {
                let rows = Arc::clone(&rows);
                async move {
                    let mut rows = rows
                        .lock()
                        .map_err(|_| vortex_err!("Build row recorder poisoned"))?;
                    if batch.rows.len() > 100_000 - rows.len() {
                        vortex_bail!("Build row order exceeds row budget");
                    }
                    rows.extend_from_slice(&batch.rows);
                    Ok(batch)
                }
            },
        )))
    }

    async fn take(&self, rows: &[RowAddress], fields: &[String]) -> VortexResult<SourceBatch> {
        self.source.take(rows, fields).await
    }
}

pub(crate) struct PreparedIndex {
    capture: Capture,
    snapshot: Snapshot,
    descriptor: LocalGeneration,
    rows: Vec<RowAddress>,
    workspace: PathBuf,
    reference: PathBuf,
    generation: String,
}

pub(crate) async fn prepare(
    source: Arc<PhysicalSource>,
    reference: &Path,
    generation: String,
    workspace: &Path,
    session: VortexSession,
    check: &dyn Fn(&str) -> VortexResult<()>,
) -> VortexResult<PreparedIndex> {
    let scratch = workspace.join("scratch");
    std::fs::create_dir(&scratch)?;
    check("scratch_created")?;
    let rows = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::new(RecordedSource {
        source: FileSource {
            snapshot: file_index::snapshot(&source, source.capture.files.clone())?,
            source: Arc::clone(&source),
        },
        rows: Arc::clone(&rows),
    });
    let (snapshot, descriptor) = build_generation_at(
        recorded,
        source.field.clone(),
        source.capture.dimension,
        BuildTarget {
            root: workspace,
            generation: "prepared".into(),
            scratch: &scratch,
        },
        session,
        check,
    )
    .await?;
    let rows = std::mem::take(
        &mut *rows
            .lock()
            .map_err(|_| vortex_err!("Build row recorder poisoned"))?,
    );
    Ok(PreparedIndex {
        capture: source.capture.clone(),
        snapshot,
        descriptor,
        rows,
        workspace: workspace.to_owned(),
        reference: reference.to_owned(),
        generation,
    })
}

impl PreparedIndex {
    pub(crate) async fn bind(
        &self,
        old_id: u64,
        new_id: u64,
        check: &dyn Fn(&str) -> VortexResult<()>,
    ) -> VortexResult<()> {
        check("rewrite_bind_started")?;
        let (store, mut metadata) =
            LocalIndexStore::open(&self.workspace, &self.descriptor, &self.snapshot, LIMITS)?;
        if self.snapshot.file(old_id).is_none()
            || (new_id != old_id && self.snapshot.file(new_id).is_some())
        {
            vortex_bail!("Final rewrite file ID is absent or overlaps retained input");
        }
        let mut snapshot = self.snapshot.clone();
        snapshot
            .files
            .iter_mut()
            .find(|file| file.id == old_id)
            .ok_or_else(|| vortex_err!("Rewrite output absent"))?
            .id = new_id;
        snapshot.files.sort_by_key(|file| file.id);
        snapshot.version = file_version(
            &serde_json::to_vec(&snapshot.files).map_err(|error| vortex_err!("{error}"))?,
        );
        snapshot.validate()?;
        let rows = self
            .rows
            .iter()
            .map(|row| RowAddress {
                file_id: if row.file_id == old_id {
                    new_id
                } else {
                    row.file_id
                },
                row_offset: row.row_offset,
            })
            .collect::<Vec<_>>();
        let native = metadata
            .artifacts
            .iter()
            .filter(|artifact| artifact.path.starts_with("spfresh/native/"))
            .cloned()
            .collect::<Vec<_>>();
        let scratch = self.workspace.join("scratch");
        let lease = store.materialize(&native, &scratch, 64 * 1024 * 1024)?;
        let bundle = SpFreshBundle {
            dimension: self.capture.dimension,
            rows: u32::try_from(rows.len())?,
            posting_page_limit: POSTING_PAGE_LIMIT,
        };
        metadata.snapshot = snapshot.clone();
        metadata.covered_files = snapshot.files.iter().map(|file| file.id).collect();
        metadata.generation = self.generation.clone();
        metadata.artifacts.clear();
        let root = self
            .reference
            .parent()
            .ok_or_else(|| vortex_err!("Reference parent required"))?;
        let bound = Arc::new(LocalIndexStore::create(root, &self.generation, LIMITS)?);
        check("rewrite_bind_created")?;
        let metadata: IndexMetadata = import_bundle(
            metadata,
            bound.as_ref(),
            &lease.path().join("spfresh/native"),
            bundle,
            &rows,
        )
        .await?;
        let descriptor = bound.seal(&metadata)?;
        check("rewrite_bind_sealed")?;
        let (verified, metadata) = LocalIndexStore::open(root, &descriptor, &snapshot, LIMITS)?;
        drop(
            provider(&scratch)?
                .open(&metadata, Arc::new(verified))
                .await?,
        );
        file_index::finish_reference(
            &self.capture,
            &self.reference,
            snapshot,
            descriptor,
            &self.workspace,
            check,
        )?;
        check("rewrite_bound")
    }
}
