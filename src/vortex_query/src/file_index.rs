//! Index complete immutable files, independently of a DuckLake query view.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::Deserialize;
use serde::Serialize;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::session::VortexSession;
use vortex_index::IndexSource;
use vortex_index::RowAddress;
use vortex_index::Snapshot;
use vortex_index::SourceBatch;
use vortex_index::SourceFile;
use vortex_index::file::file_version;
use vortex_index::store::LocalGeneration;

use crate::build::build_generation_checked;
use crate::source::PhysicalSource;
use crate::write_reference;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Reference {
    format_version: u32,
    table_uuid: String,
    field_id: u32,
    dimension: u32,
    metric: String,
    pub snapshot: Snapshot,
    pub generation: LocalGeneration,
}

fn snapshot(source: &PhysicalSource, files: Vec<SourceFile>) -> VortexResult<Snapshot> {
    let inventory = serde_json::to_vec(&files).map_err(|error| vortex_err!("{error}"))?;
    let snapshot = Snapshot {
        dataset_id: format!("ducklake-physical-files:{}", source.capture.table_uuid),
        version: file_version(&inventory),
        schema_fingerprint: format!(
            "ducklake-file-vector-v1:{}:float32:{}:squared_l2",
            source.capture.field_id, source.capture.dimension,
        ),
        files,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

pub fn reference_for(source: &PhysicalSource, reference: &Path) -> VortexResult<Reference> {
    let descriptor: Reference =
        serde_json::from_slice(&fs::read(reference)?).map_err(|error| vortex_err!("{error}"))?;
    validate_reference(source, &descriptor)?;
    Ok(descriptor)
}

pub fn validate_reference(source: &PhysicalSource, descriptor: &Reference) -> VortexResult<()> {
    if descriptor.format_version != 1
        || descriptor.table_uuid != source.capture.table_uuid
        || descriptor.field_id != source.capture.field_id
        || descriptor.dimension != source.capture.dimension
        || descriptor.metric != "squared_l2"
        || descriptor.snapshot.files.is_empty()
    {
        vortex_bail!("File index table, field or vector specification mismatch");
    }
    let files = descriptor
        .snapshot
        .files
        .iter()
        .map(|indexed| {
            let current = source.snapshot().file(indexed.id).ok_or_else(|| {
                vortex_err!("Referenced index file is absent from this query view")
            })?;
            if current != indexed {
                vortex_bail!("Referenced index file identity mismatch");
            }
            Ok(current.clone())
        })
        .collect::<VortexResult<Vec<_>>>()?;
    if snapshot(source, files)? != descriptor.snapshot {
        vortex_bail!("File index physical snapshot mismatch");
    }
    Ok(())
}

struct FileSource {
    source: Arc<PhysicalSource>,
    snapshot: Snapshot,
}

#[async_trait]
impl IndexSource for FileSource {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn scan(
        &self,
        files: &[u64],
        fields: &[String],
    ) -> VortexResult<BoxStream<'static, VortexResult<SourceBatch>>> {
        if files.iter().any(|id| self.snapshot.file(*id).is_none()) {
            vortex_bail!("Scan is outside the indexed physical files");
        }
        self.source.scan(files, fields)
    }

    async fn take(&self, rows: &[RowAddress], fields: &[String]) -> VortexResult<SourceBatch> {
        for row in rows {
            self.snapshot.validate_row(*row)?;
        }
        self.source.take(rows, fields).await
    }
}

pub async fn build(
    source: Arc<PhysicalSource>,
    reference: &Path,
    session: VortexSession,
) -> VortexResult<()> {
    build_checked(source, reference, session, &|_| Ok(())).await
}

pub(crate) async fn build_checked(
    source: Arc<PhysicalSource>,
    reference: &Path,
    session: VortexSession,
    check: &dyn Fn(&str) -> VortexResult<()>,
) -> VortexResult<()> {
    if reference.exists() {
        vortex_bail!("Index reference already exists");
    }
    let root = reference
        .parent()
        .ok_or_else(|| vortex_err!("Reference parent required"))?;
    let (snapshot, generation) = build_generation_checked(
        Arc::new(FileSource {
            snapshot: snapshot(&source, source.snapshot().files.clone())?,
            source: Arc::clone(&source),
        }),
        source.field.clone(),
        source.capture.dimension,
        root,
        session,
        check,
    )
    .await?;
    let descriptor = Reference {
        format_version: 1,
        table_uuid: source.capture.table_uuid.clone(),
        field_id: source.capture.field_id,
        dimension: source.capture.dimension,
        metric: "squared_l2".to_owned(),
        snapshot,
        generation,
    };
    check("before_reference")?;
    write_reference(reference, &descriptor)?;
    check("reference_ready")
}
