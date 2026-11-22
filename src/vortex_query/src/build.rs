//! Explicit sealed-generation builder shared with the validation probe.
use crate::{LIMITS, provider};
use bytes::Bytes;
use serde::Serialize;
use std::{fs, io::Write, path::Path, sync::Arc};
use vortex::{
    error::{VortexResult, vortex_bail, vortex_err},
    session::VortexSession,
};
use vortex_index::store::{LocalGeneration, LocalIndexStore};
use vortex_index::{
    IndexBuildRequest, IndexBuilder, IndexMetadata, IndexProvider, IndexSource, IndexStore,
    Snapshot,
};
use vortex_index_spfresh::{
    SPFRESH_ID, SpFreshBuildLimits, SpFreshBuildOptions, SpFreshIndexBuilder,
};
pub(crate) const POSTING_PAGE_LIMIT: u32 = 12;

fn build_options(snapshot: &Snapshot, dimension: u32) -> VortexResult<SpFreshBuildOptions> {
    let rows = snapshot.files.iter().try_fold(0u64, |rows, file| {
        rows.checked_add(file.row_count)
            .ok_or_else(|| vortex_err!("SPFresh physical row count overflow"))
    })?;
    if !(65..=100_000).contains(&rows) {
        vortex_bail!("SPFresh build requires 65..=100000 physical rows");
    }
    let replicas = 4;
    let stride = 4 + u64::from(dimension) * 4;
    // Target quarter-full postings at the maximum replica count; skew still fails coverage validation.
    let heads = (rows * replicas * stride * 4)
        .div_ceil(u64::from(POSTING_PAGE_LIMIT) * 4096)
        .clamp(64, rows - 1);
    Ok(SpFreshBuildOptions {
        format_version: 1,
        dimension,
        head_count: heads as u32,
        posting_page_limit: POSTING_PAGE_LIMIT,
        replicas: replicas as u32,
    })
}
pub async fn build_generation(
    source: Arc<dyn IndexSource>,
    field: String,
    dimension: u32,
    root: &Path,
    session: VortexSession,
) -> VortexResult<(Snapshot, LocalGeneration)> {
    build_generation_checked(source, field, dimension, root, session, &|_| Ok(())).await
}

pub(crate) async fn build_generation_checked(
    source: Arc<dyn IndexSource>,
    field: String,
    dimension: u32,
    root: &Path,
    session: VortexSession,
    check: &dyn Fn(&str) -> VortexResult<()>,
) -> VortexResult<(Snapshot, LocalGeneration)> {
    let scratch = tempfile::Builder::new()
        .prefix(".scratch-")
        .tempdir_in(root)?;
    let generation = format!(
        "generation-{}",
        scratch
            .path()
            .file_name()
            .ok_or_else(|| vortex_err!("Scratch name required"))?
            .to_string_lossy()
    );
    build_generation_at(
        source,
        field,
        dimension,
        BuildTarget {
            root,
            generation,
            scratch: scratch.path(),
        },
        session,
        check,
    )
    .await
}

pub(crate) struct BuildTarget<'a> {
    pub root: &'a Path,
    pub generation: String,
    pub scratch: &'a Path,
}

pub(crate) async fn build_generation_at(
    source: Arc<dyn IndexSource>,
    field: String,
    dimension: u32,
    target: BuildTarget<'_>,
    session: VortexSession,
    check: &dyn Fn(&str) -> VortexResult<()>,
) -> VortexResult<(Snapshot, LocalGeneration)> {
    check("build_started")?;
    let BuildTarget {
        root,
        generation,
        scratch,
    } = target;
    let store = Arc::new(LocalIndexStore::create(root, &generation, LIMITS)?);
    check("generation_created")?;
    let metadata = IndexMetadata {
        format_version: 1,
        name: field.clone(),
        generation,
        backend: SPFRESH_ID.into(),
        backend_version: 1,
        snapshot: source.snapshot().clone(),
        fields: vec![field],
        covered_files: source.snapshot().files.iter().map(|file| file.id).collect(),
        artifacts: vec![],
    };
    let limits = SpFreshBuildLimits {
        max_rows: 100_000,
        max_vector_bytes: 64 * 1024 * 1024,
        ..SpFreshBuildLimits::default()
    };
    let builder = SpFreshIndexBuilder::try_new(scratch.to_path_buf(), session, limits)?;
    let options = build_options(&metadata.snapshot, dimension)?;
    let expected = metadata.clone();
    let metadata = builder
        .build(
            IndexBuildRequest {
                metadata,
                backend_options: Bytes::from(
                    serde_json::to_vec(&options).map_err(|error| vortex_err!("{error}"))?,
                ),
            },
            source,
            Arc::clone(&store) as Arc<dyn IndexStore>,
        )
        .await?;
    let mut identity = metadata.clone();
    check("build_complete")?;
    identity.artifacts.clear();
    if identity != expected {
        vortex_bail!("Provider changed requested identity or coverage");
    }
    let generation = store.seal(&metadata)?;
    check("generation_sealed")?;
    let (verified, reopened) =
        LocalIndexStore::open(root, &generation, &metadata.snapshot, LIMITS)?;
    drop(
        provider(scratch)?
            .open(&reopened, Arc::new(verified))
            .await?,
    );
    Ok((metadata.snapshot, generation))
}

pub fn write_reference(reference: &Path, descriptor: &impl Serialize) -> VortexResult<()> {
    let root = reference
        .parent()
        .ok_or_else(|| vortex_err!("Reference parent required"))?;
    write_reference_at(reference, descriptor, root, &|_| Ok(()))
}

pub(crate) fn write_reference_at(
    reference: &Path,
    descriptor: &impl Serialize,
    temporary: &Path,
    check: &dyn Fn(&str) -> VortexResult<()>,
) -> VortexResult<()> {
    let root = reference
        .parent()
        .ok_or_else(|| vortex_err!("Reference parent required"))?;
    let mut pending = tempfile::NamedTempFile::new_in(temporary)?;
    serde_json::to_writer(&mut pending, descriptor).map_err(|error| vortex_err!("{error}"))?;
    pending.flush()?;
    pending.as_file().sync_all()?;
    check("reference_written")?;
    pending
        .persist_noclobber(reference)
        .map_err(|error| vortex_err!("{error}"))?;
    fs::File::open(root)?.sync_all()?;
    fs::File::open(temporary)?.sync_all()?;
    Ok(())
}
