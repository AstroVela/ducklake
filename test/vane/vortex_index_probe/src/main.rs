//! Test-only DuckLake bridge to the existing Vortex SPFresh provider.

mod read_view;
mod source;

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use source::PhysicalSource;
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::FixedSizeListArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::array::arrays::fixed_size_list::FixedSizeListArraySlotsExt;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::io::runtime::single::block_on;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex_index::Index;
use vortex_index::IndexBuildRequest;
use vortex_index::IndexBuilder;
use vortex_index::IndexMetadata;
use vortex_index::IndexProvider;
use vortex_index::IndexSource;
use vortex_index::IndexStore;
use vortex_index::RowAddress;
use vortex_index::RowFilter;
use vortex_index::SearchMode;
use vortex_index::Snapshot;
use vortex_index::VectorSearchOptions;
use vortex_index::store::LocalGeneration;
use vortex_index::store::LocalIndexStore;
use vortex_index::store::LocalStoreLimits;
use vortex_index_spfresh::SPFRESH_ID;
use vortex_index_spfresh::SpFreshBuildLimits;
use vortex_index_spfresh::SpFreshIndexBuilder;
use vortex_index_spfresh::SpFreshLimits;
use vortex_index_spfresh::SpFreshProvider;

const LIMITS: LocalStoreLimits = LocalStoreLimits {
    max_artifact_bytes: 64 * 1024 * 1024,
    max_manifest_bytes: 1024 * 1024,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    format_version: u32,
    capture_digest: String,
    snapshot: Snapshot,
    generation: LocalGeneration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    query: Vec<f32>,
    k: usize,
    allow: Option<Vec<RowAddress>>,
    #[serde(default)]
    exact: bool,
}

#[derive(Serialize)]
struct Hit {
    file_id: u64,
    row_offset: u64,
    distance: f32,
    embedding: Vec<f32>,
}

fn provider(scratch: &Path) -> VortexResult<SpFreshProvider> {
    SpFreshProvider::try_new(scratch.to_path_buf(), SpFreshLimits::default())
}

async fn build(
    source: Arc<PhysicalSource>,
    reference: &Path,
    session: VortexSession,
) -> VortexResult<()> {
    if reference.exists() {
        vortex_bail!("Index reference already exists");
    }
    let root = reference
        .parent()
        .ok_or_else(|| vortex_err!("Reference parent required"))?;
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
    let store = Arc::new(LocalIndexStore::create(root, &generation, LIMITS)?);
    let metadata = IndexMetadata {
        format_version: 1,
        name: format!("field-{}", source.capture.field_id),
        generation,
        backend: SPFRESH_ID.into(),
        backend_version: 1,
        snapshot: source.snapshot().clone(),
        fields: vec![source.field.clone()],
        covered_files: source.snapshot().files.iter().map(|file| file.id).collect(),
        artifacts: vec![],
    };
    let limits = SpFreshBuildLimits {
        max_rows: 100_000,
        max_vector_bytes: 64 * 1024 * 1024,
        ..SpFreshBuildLimits::default()
    };
    let builder = SpFreshIndexBuilder::try_new(scratch.path().to_path_buf(), session, limits)?;
    let options = serde_json::json!({"format_version":1, "dimension":source.capture.dimension,
        "head_count":64, "posting_page_limit":12, "replicas":4});
    let expected = metadata.clone();
    let digest = source.digest.clone();
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
    identity.artifacts.clear();
    if identity != expected {
        vortex_bail!("Provider changed requested identity or coverage");
    }
    let generation = store.seal(&metadata)?;
    let (verified, reopened) =
        LocalIndexStore::open(root, &generation, &metadata.snapshot, LIMITS)?;
    drop(
        provider(scratch.path())?
            .open(&reopened, Arc::new(verified))
            .await?,
    );
    let descriptor = Reference {
        format_version: 1,
        capture_digest: digest,
        snapshot: metadata.snapshot,
        generation,
    };
    let mut pending = tempfile::NamedTempFile::new_in(root)?;
    serde_json::to_writer(&mut pending, &descriptor).map_err(|error| vortex_err!("{error}"))?;
    pending.flush()?;
    pending.as_file().sync_all()?;
    pending
        .persist_noclobber(reference)
        .map_err(|error| vortex_err!("{error}"))?;
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn reference_for(source: &PhysicalSource, reference: &Path) -> VortexResult<Reference> {
    let descriptor: Reference =
        serde_json::from_slice(&fs::read(reference)?).map_err(|error| vortex_err!("{error}"))?;
    if descriptor.format_version != 1
        || descriptor.capture_digest != source.digest
        || descriptor.snapshot != *source.snapshot()
    {
        vortex_bail!("DuckLake snapshot does not match the index capture");
    }
    Ok(descriptor)
}

async fn query_hits(
    source: &PhysicalSource,
    index: &dyn Index,
    request: Request,
    session: VortexSession,
) -> VortexResult<Vec<Hit>> {
    let k = NonZeroUsize::new(request.k).ok_or_else(|| vortex_err!("k must be nonzero"))?;
    let visible: BTreeSet<_> = source.capture.visible.iter().copied().collect();
    let excluded = source
        .snapshot()
        .files
        .iter()
        .flat_map(|file| {
            (0..file.row_count).map(move |row_offset| RowAddress {
                file_id: file.id,
                row_offset,
            })
        })
        .filter(|row| !visible.contains(row))
        .collect();
    let allow = request.allow.map(|rows| rows.into_iter().collect());
    let filter = RowFilter::try_new(source.snapshot().clone(), allow, excluded)?;
    let vector = index
        .as_vector()
        .ok_or_else(|| vortex_err!("Vector capability required"))?;
    if !vector.supports_filter() {
        vortex_bail!("Provider lacks mandatory prefilter support");
    }
    let hits = vector
        .search(
            &request.query,
            &VectorSearchOptions {
                k,
                mode: if request.exact {
                    SearchMode::Exact
                } else {
                    SearchMode::Approximate
                },
                backend_options: Bytes::from_static(
                    b"{\"max_check\":4096,\"internal_results\":64,\"search_pages\":12}",
                ),
            },
            &filter,
        )
        .await?;
    let rows: Vec<_> = hits.iter().map(|hit| hit.row).collect();
    if hits.len() > request.k
        || rows.iter().collect::<BTreeSet<_>>().len() != rows.len()
        || rows
            .iter()
            .any(|row| !visible.contains(row) || !filter.includes(*row))
    {
        vortex_bail!("Provider returned a duplicate, invisible or disallowed row");
    }
    let taken = source
        .take(&rows, std::slice::from_ref(&source.field))
        .await?;
    if taken.rows != rows {
        vortex_bail!("Ranked take changed physical row order");
    }
    let mut context = session.create_execution_ctx();
    let data = taken.data.execute::<StructArray>(&mut context)?;
    let lists = data
        .unmasked_field(0)
        .clone()
        .execute::<FixedSizeListArray>(&mut context)?;
    let elements = lists
        .elements()
        .clone()
        .execute::<PrimitiveArray>(&mut context)?;
    let dimension = source.capture.dimension as usize;
    let values = elements.as_slice::<f32>();
    let result: Vec<_> = hits
        .into_iter()
        .enumerate()
        .map(|(rank, hit)| Hit {
            file_id: hit.row.file_id,
            row_offset: hit.row.row_offset,
            distance: hit.distance,
            embedding: values[rank * dimension..(rank + 1) * dimension].to_vec(),
        })
        .collect();
    Ok(result)
}

async fn search(
    source: Arc<PhysicalSource>,
    reference: &Path,
    request: &Path,
    output: &Path,
    session: VortexSession,
) -> VortexResult<()> {
    let descriptor = reference_for(&source, reference)?;
    let request: Request =
        serde_json::from_slice(&fs::read(request)?).map_err(|error| vortex_err!("{error}"))?;
    let root = reference
        .parent()
        .ok_or_else(|| vortex_err!("Reference parent required"))?;
    let scratch = tempfile::Builder::new()
        .prefix(".scratch-")
        .tempdir_in(root)?;
    let (store, metadata) =
        LocalIndexStore::open(root, &descriptor.generation, source.snapshot(), LIMITS)?;
    let index = provider(scratch.path())?
        .open(&metadata, Arc::new(store))
        .await?;
    let result = query_hits(&source, index.as_ref(), request, session).await?;
    fs::write(
        output,
        serde_json::to_vec(&result).map_err(|error| vortex_err!("{error}"))?,
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().collect();
    let command = args.get(1).cloned().unwrap_or_default();
    if !matches!(
        (command.as_str(), args.len()),
        ("build", 4) | ("search", 6) | ("serve", 4)
    ) {
        return Err(
            "usage: probe build CAPTURE REFERENCE | search CAPTURE REFERENCE REQUEST OUTPUT | serve ROOT LIMITS_JSON".into(),
        );
    }
    block_on(|handle| async move {
        let session = VortexSession::default().with_handle(handle);
        if command == "serve" {
            return read_view::serve(Path::new(&args[2]), &args[3], session).await;
        }
        let source = Arc::new(PhysicalSource::open(&fs::read(&args[2])?, session.clone()).await?);
        let reference = Path::new(&args[3]);
        if !reference.is_absolute() {
            vortex_bail!("Reference must be absolute");
        }
        if command == "build" {
            build(source, reference, session).await
        } else {
            search(
                source,
                reference,
                Path::new(&args[4]),
                Path::new(&args[5]),
                session,
            )
            .await
        }
    })?;
    Ok(())
}
