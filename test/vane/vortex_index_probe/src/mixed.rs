//! Explicit B (ANN files), F (scan files), T (inlined rows) query plan.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::FixedSizeListArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::array::arrays::fixed_size_list::FixedSizeListArraySlotsExt;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::session::VortexSession;
use vortex_index::DistanceMetric;
use vortex_index::IndexSource;
use vortex_index::RowAddress;
use vortex_index::RowFilter;
use vortex_index::SearchMode;
use vortex_index::Snapshot;
use vortex_index::VectorSearchOptions;
use vortex_index::file::file_version;
use vortex_index_spfresh::SPFRESH_ID;

use crate::admission;
use crate::file_index;
use crate::read_view::IndexLease;
use crate::read_view::ViewBudget;
use crate::source::BusinessRow;
use crate::source::Capture;
use crate::source::PhysicalSource;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RowRef {
    File { file_id: u64, row_offset: u64 },
    Inline { source: String, row_id: u64 },
}

impl From<RowAddress> for RowRef {
    fn from(row: RowAddress) -> Self {
        Self::File {
            file_id: row.file_id,
            row_offset: row.row_offset,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InlineRow {
    row_ref: RowRef,
    row: BusinessRow,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixedCapture {
    pub format_version: u32,
    pub file_capture: Capture,
    inline_rows: Vec<InlineRow>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixedRequest {
    query: Vec<f32>,
    k: usize,
    allow: Option<Vec<RowRef>>,
}

#[derive(Clone)]
struct Candidate {
    row_ref: RowRef,
    distance: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.row_ref.cmp(&other.row_ref))
    }
}

struct TopK {
    k: usize,
    heap: BinaryHeap<Candidate>,
}

impl TopK {
    fn new(k: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k),
        }
    }

    fn offer(&mut self, candidate: Candidate) {
        if self.heap.len() < self.k {
            self.heap.push(candidate);
        } else if self.heap.peek().is_some_and(|worst| &candidate < worst) {
            self.heap.pop();
            self.heap.push(candidate);
        }
    }
}

fn squared_l2(query: &[f32], vector: &[f32]) -> VortexResult<f32> {
    let distance = query.iter().zip(vector).fold(0.0f32, |total, (a, b)| {
        let difference = a - b;
        total + difference * difference
    });
    if !distance.is_finite() {
        vortex_bail!("Squared L2 distance overflow");
    }
    Ok(distance)
}

#[derive(Serialize)]
pub struct MixedHit {
    row_ref: RowRef,
    distance: f32,
    row: BusinessRow,
}

#[derive(Serialize)]
pub struct Routes {
    indexed_files: Vec<u64>,
    scan_files: Vec<u64>,
    ann_calls: usize,
    ann_candidates: usize,
    scanned_rows: usize,
    scan_eligible_rows: usize,
    inline_rows: usize,
    inline_eligible_rows: usize,
    merge_candidates: usize,
}

#[derive(Serialize)]
pub struct MixedResult {
    hits: Vec<MixedHit>,
    routes: Routes,
}

pub struct MixedView {
    pub lease: IndexLease,
    pub source: Arc<PhysicalSource>,
    pub digest: String,
    pub inline_bytes: usize,
    inline: BTreeMap<RowRef, BusinessRow>,
    visible: BTreeSet<RowAddress>,
    indexed: Snapshot,
    scan_files: Vec<u64>,
    pub catalog_revision: Option<u64>,
}

pub enum ViewInput {
    Mixed { reference: PathBuf },
    Admitted { reference: PathBuf },
    Catalogued { index_name: String },
}

impl MixedView {
    pub async fn open(
        capture: &Path,
        input: ViewInput,
        leases: &Path,
        budget: ViewBudget,
        session: VortexSession,
    ) -> VortexResult<Self> {
        if !capture.is_absolute() {
            vortex_bail!("Retained view paths must be absolute");
        }
        let admitted = !matches!(&input, ViewInput::Mixed { .. });
        let max_bytes = if admitted { 160 } else { 64 } * 1024 * 1024;
        if fs::metadata(capture)?.len() > max_bytes {
            vortex_bail!("Mixed capture exceeds its byte budget");
        }
        let bytes = fs::read(capture)?;
        let mut catalog = None;
        let (captured, pins) = if let ViewInput::Catalogued { index_name } = &input {
            let (prepared, index) =
                admission::prepare_catalogued(&bytes, index_name, budget.source_bytes, leases)?;
            catalog = Some(index);
            (prepared.capture, Some(prepared.pins))
        } else if admitted {
            let prepared = admission::prepare(&bytes, budget.source_bytes, leases)?;
            (prepared.capture, Some(prepared.pins))
        } else {
            (
                serde_json::from_slice::<MixedCapture>(&bytes)
                    .map_err(|error| vortex_err!("{error}"))?,
                None,
            )
        };
        if captured.format_version != 1 || captured.file_capture.business_fields.is_none() {
            vortex_bail!("Mixed capture requires version 1 and business field IDs");
        }
        if captured.inline_rows.len() > budget.inline_rows {
            vortex_bail!("Retained inline row budget exceeded");
        }
        let inline_bytes = if captured.inline_rows.is_empty() {
            0
        } else {
            serde_json::to_vec(&captured.inline_rows)
                .map_err(|error| vortex_err!("{error}"))?
                .len()
        };
        if inline_bytes > budget.inline_bytes {
            vortex_bail!("Retained inline byte budget exceeded");
        }
        let mut inline = BTreeMap::new();
        for value in captured.inline_rows {
            let RowRef::Inline { source, .. } = &value.row_ref else {
                vortex_bail!("Inlined rows require an inline address, never a file ID");
            };
            if !source.starts_with("ducklake_inlined_data_")
                || source.len() > 256
                || value.row.embedding.len() != captured.file_capture.dimension as usize
                || value
                    .row
                    .embedding
                    .iter()
                    .any(|component| !component.is_finite())
            {
                vortex_bail!("Invalid inline identity or vector dimension/components");
            }
            if inline.insert(value.row_ref, value.row).is_some() {
                vortex_bail!("Duplicate inline row address");
            }
        }
        let source_capture =
            serde_json::to_vec(&captured.file_capture).map_err(|error| vortex_err!("{error}"))?;
        let source = Arc::new(match &pins {
            Some(pins) => {
                PhysicalSource::open_pinned(
                    &source_capture,
                    session,
                    budget.source_bytes,
                    &pins.files,
                )
                .await?
            }
            None => {
                PhysicalSource::open_with_budget(&source_capture, session, budget.source_bytes)
                    .await?
            }
        });
        drop(pins);
        let catalog_revision = catalog.as_ref().map(|index| index.revision);
        let (descriptor, root) = match input {
            ViewInput::Mixed { reference } | ViewInput::Admitted { reference } => {
                if !reference.is_absolute() {
                    vortex_bail!("Retained view paths must be absolute");
                }
                let descriptor = file_index::reference_for(&source, &reference)?;
                let root = reference
                    .parent()
                    .ok_or_else(|| vortex_err!("Reference parent required"))?
                    .to_path_buf();
                (descriptor, root)
            }
            ViewInput::Catalogued { .. } => {
                let index =
                    catalog.ok_or_else(|| vortex_err!("Catalogued index record required"))?;
                file_index::validate_reference(&source, &index.reference)?;
                (index.reference, index.root)
            }
        };
        let scan_files = source
            .snapshot()
            .files
            .iter()
            .filter(|file| descriptor.snapshot.file(file.id).is_none())
            .map(|file| file.id)
            .collect();
        let lease = IndexLease::open_root(
            &root,
            &descriptor.generation,
            &descriptor.snapshot,
            leases,
            budget.artifact_bytes,
        )
        .await?;
        let metadata = lease.index.metadata();
        let vector = lease
            .index
            .as_vector()
            .ok_or_else(|| vortex_err!("Vector capability required"))?;
        if metadata.backend != SPFRESH_ID
            || metadata.fields != [source.field.clone()]
            || metadata.covered_files
                != descriptor
                    .snapshot
                    .files
                    .iter()
                    .map(|file| file.id)
                    .collect::<Vec<_>>()
            || vector.spec().dimension.get() != source.capture.dimension as usize
            || vector.spec().metric != DistanceMetric::SquaredL2
            || !vector.supports_filter()
        {
            vortex_bail!("File index coverage, field, metric or prefilter capability mismatch");
        }
        let visible = source.capture.visible.iter().copied().collect();
        Ok(Self {
            lease,
            source,
            digest: file_version(&bytes),
            inline_bytes,
            inline,
            visible,
            indexed: descriptor.snapshot,
            scan_files,
            catalog_revision,
        })
    }

    pub fn inline_rows(&self) -> usize {
        self.inline.len()
    }

    pub fn check_digest(&self, digest: &str) -> VortexResult<()> {
        if self.digest != digest {
            vortex_bail!("Read view capture mismatch");
        }
        Ok(())
    }

    fn contains(&self, row: &RowRef) -> bool {
        match row {
            RowRef::File {
                file_id,
                row_offset,
            } => self.visible.contains(&RowAddress {
                file_id: *file_id,
                row_offset: *row_offset,
            }),
            RowRef::Inline { .. } => self.inline.contains_key(row),
        }
    }

    pub async fn fetch(&self, rows: &[RowRef]) -> VortexResult<Vec<BusinessRow>> {
        if rows.len() > 4096 {
            vortex_bail!("Retained fetch exceeds its row budget");
        }
        if rows.iter().any(|row| !self.contains(row)) {
            vortex_bail!("Retained fetch requires visible addresses from this read view");
        }
        let files: Vec<_> = rows
            .iter()
            .filter_map(|row| match row {
                RowRef::File {
                    file_id,
                    row_offset,
                } => Some(RowAddress {
                    file_id: *file_id,
                    row_offset: *row_offset,
                }),
                RowRef::Inline { .. } => None,
            })
            .collect();
        let mut fetched = self.source.take_business(&files).await?.into_iter();
        rows.iter()
            .map(|row| match row {
                RowRef::File { .. } => fetched
                    .next()
                    .ok_or_else(|| vortex_err!("Missing file business row")),
                RowRef::Inline { .. } => self
                    .inline
                    .get(row)
                    .cloned()
                    .ok_or_else(|| vortex_err!("Missing inline business row")),
            })
            .collect()
    }

    pub async fn search(
        &self,
        request: MixedRequest,
        session: VortexSession,
    ) -> VortexResult<MixedResult> {
        if !(1..=64).contains(&request.k) {
            vortex_bail!("Mixed k must be between 1 and 64");
        }
        let dimension = self.source.capture.dimension as usize;
        if request.query.len() != dimension || request.query.iter().any(|value| !value.is_finite())
        {
            vortex_bail!("Query dimension or finite components mismatch");
        }
        let allow = request
            .allow
            .map(|rows| rows.into_iter().collect::<BTreeSet<_>>());
        if allow.iter().flatten().any(|row| !self.contains(row)) {
            vortex_bail!("Prefilter requires visible addresses from this read view");
        }
        let allowed = |row: &RowRef| allow.as_ref().is_none_or(|rows| rows.contains(row));
        let b_allow = allow.as_ref().map(|rows| {
            rows.iter()
                .filter_map(|row| match row {
                    RowRef::File {
                        file_id,
                        row_offset,
                    } if self.indexed.file(*file_id).is_some() => Some(RowAddress {
                        file_id: *file_id,
                        row_offset: *row_offset,
                    }),
                    _ => None,
                })
                .collect()
        });
        let excluded = self
            .indexed
            .files
            .iter()
            .flat_map(|file| {
                (0..file.row_count).map(move |row_offset| RowAddress {
                    file_id: file.id,
                    row_offset,
                })
            })
            .filter(|row| !self.visible.contains(row))
            .collect();
        let filter = RowFilter::try_new(self.indexed.clone(), b_allow, excluded)?;
        let vector = self
            .lease
            .index
            .as_vector()
            .ok_or_else(|| vortex_err!("Vector capability required"))?;
        let hits = vector
            .search(
                &request.query,
                &VectorSearchOptions {
                    k: NonZeroUsize::new(request.k)
                        .ok_or_else(|| vortex_err!("k must be nonzero"))?,
                    mode: SearchMode::Approximate,
                    backend_options: Bytes::from_static(
                        b"{\"max_check\":4096,\"internal_results\":64,\"search_pages\":12}",
                    ),
                },
                &filter,
            )
            .await?;
        let b_rows: Vec<_> = hits.iter().map(|hit| hit.row).collect();
        if hits.len() > request.k || b_rows.iter().collect::<BTreeSet<_>>().len() != b_rows.len() {
            vortex_bail!("ANN returned duplicate or excess candidates");
        }
        for hit in &hits {
            self.indexed.validate_row(hit.row)?;
            if !self.visible.contains(&hit.row)
                || !filter.includes(hit.row)
                || !hit.distance.is_finite()
                || hit.distance < 0.0
            {
                vortex_bail!("ANN returned an invisible, disallowed or invalid candidate");
            }
        }
        let b_business = self.source.take_business(&b_rows).await?;
        let mut b = TopK::new(request.k);
        for (row, business) in b_rows.into_iter().zip(b_business) {
            b.offer(Candidate {
                row_ref: row.into(),
                distance: squared_l2(&request.query, &business.embedding)?,
            });
        }
        let mut routes = Routes {
            indexed_files: self.indexed.files.iter().map(|file| file.id).collect(),
            scan_files: self.scan_files.clone(),
            ann_calls: 1,
            ann_candidates: hits.len(),
            scanned_rows: 0,
            scan_eligible_rows: 0,
            inline_rows: self.inline.len(),
            inline_eligible_rows: 0,
            merge_candidates: 0,
        };
        let mut f = TopK::new(request.k);
        let mut stream = self
            .source
            .scan(&self.scan_files, std::slice::from_ref(&self.source.field))?;
        let mut context = session.create_execution_ctx();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            routes.scanned_rows += batch.rows.len();
            let data = batch.data.execute::<StructArray>(&mut context)?;
            let lists = data
                .unmasked_field(0)
                .clone()
                .execute::<FixedSizeListArray>(&mut context)?;
            let elements = lists
                .elements()
                .clone()
                .execute::<PrimitiveArray>(&mut context)?;
            let values = elements.as_slice::<f32>();
            for (offset, row) in batch.rows.into_iter().enumerate() {
                let row_ref = RowRef::from(row);
                if self.visible.contains(&row) && allowed(&row_ref) {
                    routes.scan_eligible_rows += 1;
                    f.offer(Candidate {
                        row_ref,
                        distance: squared_l2(
                            &request.query,
                            &values[offset * dimension..(offset + 1) * dimension],
                        )?,
                    });
                }
            }
        }
        let mut t = TopK::new(request.k);
        for (row_ref, row) in &self.inline {
            if allowed(row_ref) {
                routes.inline_eligible_rows += 1;
                t.offer(Candidate {
                    row_ref: row_ref.clone(),
                    distance: squared_l2(&request.query, &row.embedding)?,
                });
            }
        }
        routes.merge_candidates = b.heap.len() + f.heap.len() + t.heap.len();
        let mut merged = TopK::new(request.k);
        for candidate in b.heap.into_iter().chain(f.heap).chain(t.heap) {
            merged.offer(candidate);
        }
        let ranked = merged.heap.into_sorted_vec();
        let rows: Vec<_> = ranked.iter().map(|hit| hit.row_ref.clone()).collect();
        let business = self.fetch(&rows).await?;
        let hits = ranked
            .into_iter()
            .zip(business)
            .map(|(hit, row)| MixedHit {
                row_ref: hit.row_ref,
                distance: hit.distance,
                row,
            })
            .collect();
        Ok(MixedResult { hits, routes })
    }
}
