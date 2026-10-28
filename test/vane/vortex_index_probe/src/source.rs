//! Bounded physical-file adapter for the DuckLake validation probe.
//! The provider indexes complete physical files. A mandatory query RowFilter
//! applies DuckLake visibility, which is bound by the immutable capture digest.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde::Deserialize;
use serde::Serialize;
use vortex::array::ArrayRef;
use vortex::array::ExecutionCtx;
use vortex::array::IntoArray;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::FixedSizeListArray;
use vortex::array::arrays::ListViewArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::array::arrays::VarBinViewArray;
use vortex::array::arrays::list::ListArrayExt;
use vortex::array::arrays::list::ListArraySlotsExt;
use vortex::array::arrays::listview::list_from_list_view;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::validity::Validity;
use vortex::buffer::Buffer;
use vortex::buffer::ByteBuffer;
use vortex::dtype::DType;
use vortex::dtype::Nullability;
use vortex::dtype::PType;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::file::OpenOptionsSessionExt;
use vortex::session::VortexSession;
use vortex_index::IndexSource;
use vortex_index::RowAddress;
use vortex_index::Snapshot;
use vortex_index::SourceBatch;
use vortex_index::SourceFile;
use vortex_index::file::LocalFileSource;
use vortex_index::file::file_version;
use vortex_index::file::schema_fingerprint;

const MAX_SOURCE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub format_version: u32,
    pub table_uuid: String,
    pub snapshot_id: u64,
    pub schema_version: u64,
    pub field_id: u32,
    pub dimension: u32,
    pub files: Vec<SourceFile>,
    pub visible: Vec<RowAddress>,
    #[serde(default)]
    pub business_fields: Option<BusinessFields>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BusinessFields {
    pub id: u32,
    pub category: u32,
    pub payload: u32,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BusinessRow {
    pub id: Option<i64>,
    pub embedding: Vec<f32>,
    pub category: Option<String>,
    pub payload: Option<String>,
}

fn physical_name(field: u32) -> String {
    format!("__ducklake_vortex_v1_field_{field}")
}

pub struct PhysicalSource {
    pub capture: Capture,
    pub field: String,
    pub digest: String,
    snapshot: Snapshot,
    files: BTreeMap<u64, Arc<LocalFileSource>>,
    session: VortexSession,
}

#[derive(Clone)]
struct VectorAdapter {
    dimension: u32,
    field: String,
    snapshot: Snapshot,
    session: VortexSession,
}

impl PhysicalSource {
    pub async fn open(bytes: &[u8], session: VortexSession) -> VortexResult<Self> {
        Self::open_with_budget(bytes, session, MAX_SOURCE_BYTES).await
    }

    pub async fn open_with_budget(
        bytes: &[u8],
        session: VortexSession,
        budget: usize,
    ) -> VortexResult<Self> {
        let capture: Capture =
            serde_json::from_slice(bytes).map_err(|error| vortex_err!("{error}"))?;
        if capture.format_version != 1
            || capture.table_uuid.is_empty()
            || !(1..=4096).contains(&capture.dimension)
            || capture.files.is_empty()
        {
            vortex_bail!("Invalid DuckLake capture or vector dimension");
        }
        let digest = file_version(bytes);
        let field = physical_name(capture.field_id);
        let snapshot = Snapshot {
            dataset_id: format!("ducklake-table:{}", capture.table_uuid),
            version: format!("ducklake-snapshot:{}:{digest}", capture.snapshot_id),
            schema_fingerprint: format!(
                "ducklake-vector-v1:{}:{}:{}",
                capture.schema_version, capture.field_id, capture.dimension
            ),
            files: capture.files.clone(),
        };
        snapshot.validate()?;
        let mut visible = BTreeSet::new();
        for row in &capture.visible {
            snapshot.validate_row(*row)?;
            if !visible.insert(*row) {
                vortex_bail!("Duplicate visible DuckLake row address");
            }
        }
        let mut files = BTreeMap::new();
        let mut remaining = budget.min(MAX_SOURCE_BYTES);
        for file in &snapshot.files {
            let metadata = fs::metadata(&file.uri)?;
            if !metadata.is_file() || metadata.len() > remaining as u64 {
                vortex_bail!("Physical source exceeds its local byte budget");
            }
            let bytes = fs::read(&file.uri)?;
            if bytes.len() > remaining || file_version(&bytes) != file.version {
                vortex_bail!("Physical source file version mismatch");
            }
            remaining -= bytes.len();
            let opened = session
                .open_options()
                .open_buffer(ByteBuffer::from(bytes))?;
            let dtype = opened.dtype().clone();
            if opened.row_count() != file.row_count {
                vortex_bail!("Physical source row count mismatch");
            }
            let DType::Struct(fields, Nullability::NonNullable) = &dtype else {
                vortex_bail!("DuckLake physical file requires a struct schema");
            };
            if !matches!(fields.field(&field), Some(DType::List(element, _))
                if matches!(element.as_ref(), DType::Primitive(PType::F32, _)))
            {
                vortex_bail!("DuckLake indexed field requires FLOAT[]");
            }
            if let Some(business) = &capture.business_fields {
                if [
                    capture.field_id,
                    business.id,
                    business.category,
                    business.payload,
                ]
                .into_iter()
                .collect::<BTreeSet<_>>()
                .len()
                    != 4
                {
                    vortex_bail!("Business projection requires distinct field IDs");
                }
                if !matches!(
                    fields.field(physical_name(business.id)),
                    Some(DType::Primitive(PType::I64, _))
                ) || !matches!(
                    fields.field(physical_name(business.category)),
                    Some(DType::Utf8(_))
                ) || !matches!(
                    fields.field(physical_name(business.payload)),
                    Some(DType::Utf8(_))
                ) {
                    vortex_bail!("Business projection schema does not match the capture");
                }
            }
            let mut physical_snapshot = snapshot.clone();
            physical_snapshot.schema_fingerprint = schema_fingerprint(&dtype)?;
            physical_snapshot.files = vec![file.clone()];
            let source = LocalFileSource::open(
                physical_snapshot,
                dtype,
                metadata.len() as usize,
                session.clone(),
            )
            .await?;
            files.insert(file.id, Arc::new(source));
        }
        Ok(Self {
            capture,
            field,
            digest,
            snapshot,
            files,
            session,
        })
    }

    fn validate_projection(&self, fields: &[String]) -> VortexResult<()> {
        if fields != [self.field.clone()] {
            vortex_bail!("The validation source projects its indexed field only");
        }
        Ok(())
    }

    pub fn pinned_bytes(&self) -> usize {
        self.files
            .values()
            .map(|source| source.pinned_bytes())
            .sum()
    }

    pub async fn take_business(&self, rows: &[RowAddress]) -> VortexResult<Vec<BusinessRow>> {
        let business = self
            .capture
            .business_fields
            .as_ref()
            .ok_or_else(|| vortex_err!("Retained views require captured business field IDs"))?;
        let fields = [
            self.field.clone(),
            physical_name(business.id),
            physical_name(business.category),
            physical_name(business.payload),
        ];
        let mut groups: BTreeMap<u64, Vec<(usize, RowAddress)>> = BTreeMap::new();
        for (rank, row) in rows.iter().enumerate() {
            self.snapshot.validate_row(*row)?;
            groups.entry(row.file_id).or_default().push((rank, *row));
        }
        let mut result = vec![None; rows.len()];
        let dimension = self.capture.dimension as usize;
        let mut context = self.session.create_execution_ctx();
        for (id, group) in groups {
            let selected: Vec<_> = group.iter().map(|(_, row)| *row).collect();
            let source = self
                .files
                .get(&id)
                .ok_or_else(|| vortex_err!("Unknown physical file ID"))?;
            let taken = source.take(&selected, &fields).await?;
            if taken.rows != selected {
                vortex_bail!("Business take changed row order");
            }
            let vectors = self.adapter().vectors(&taken)?;
            let data = taken.data.execute::<StructArray>(&mut context)?;
            let ids = data
                .unmasked_field(1)
                .clone()
                .execute::<PrimitiveArray>(&mut context)?;
            let validity = ids.validity()?.execute_mask(ids.len(), &mut context)?;
            let categories = strings(data.unmasked_field(2).clone(), &mut context)?;
            let payloads = strings(data.unmasked_field(3).clone(), &mut context)?;
            for (offset, (rank, _)) in group.iter().enumerate() {
                result[*rank] = Some(BusinessRow {
                    id: validity
                        .value(offset)
                        .then(|| ids.as_slice::<i64>()[offset]),
                    embedding: vectors[offset * dimension..(offset + 1) * dimension].to_vec(),
                    category: categories[offset].clone(),
                    payload: payloads[offset].clone(),
                });
            }
        }
        result
            .into_iter()
            .map(|row| row.ok_or_else(|| vortex_err!("Missing business row")))
            .collect()
    }

    fn adapter(&self) -> VectorAdapter {
        VectorAdapter {
            dimension: self.capture.dimension,
            field: self.field.clone(),
            snapshot: self.snapshot.clone(),
            session: self.session.clone(),
        }
    }
}

fn strings(array: ArrayRef, context: &mut ExecutionCtx) -> VortexResult<Vec<Option<String>>> {
    let values = array.execute::<VarBinViewArray>(context)?;
    let validity = values.validity()?.execute_mask(values.len(), context)?;
    validity
        .iter()
        .enumerate()
        .map(|(index, valid)| {
            if valid {
                Ok(Some(
                    std::str::from_utf8(&values.bytes_at(index))
                        .map_err(|error| vortex_err!("Invalid business UTF-8: {error}"))?
                        .to_owned(),
                ))
            } else {
                Ok(None)
            }
        })
        .collect()
}

impl VectorAdapter {
    fn vectors(&self, batch: &SourceBatch) -> VortexResult<Vec<f32>> {
        let mut context = self.session.create_execution_ctx();
        let data = batch.data.clone().execute::<StructArray>(&mut context)?;
        let list_view = data
            .unmasked_field(0)
            .clone()
            .execute::<ListViewArray>(&mut context)?;
        let lists = list_from_list_view(list_view, &mut context)?;
        if !lists
            .validity()?
            .execute_mask(lists.len(), &mut context)?
            .all_true()
        {
            vortex_bail!("Index vectors cannot contain NULL lists");
        }
        let lists = lists.reset_offsets(false, &mut context)?;
        let offsets = lists
            .offsets()
            .clone()
            .execute::<PrimitiveArray>(&mut context)?;
        let dimension = self.dimension as usize;
        let valid_sizes = vortex::array::match_each_integer_ptype!(offsets.ptype(), |O| {
            offsets
                .as_slice::<O>()
                .windows(2)
                .all(|pair| i128::from(pair[1]) - i128::from(pair[0]) == i128::from(self.dimension))
        });
        if !valid_sizes {
            vortex_bail!("Index vector dimension mismatch");
        }
        let elements = lists
            .elements()
            .clone()
            .execute::<PrimitiveArray>(&mut context)?;
        if elements.ptype() != PType::F32
            || !elements
                .validity()?
                .execute_mask(elements.len(), &mut context)?
                .all_true()
        {
            vortex_bail!("Index vectors cannot contain NULL components");
        }
        let values = elements.as_slice::<f32>();
        let count = lists.len() * dimension;
        if values.len() < count || values[..count].iter().any(|value| !value.is_finite()) {
            vortex_bail!("Index vectors must have finite components");
        }
        Ok(values[..count].to_vec())
    }

    fn batch(&self, rows: Vec<RowAddress>, values: Vec<f32>) -> VortexResult<SourceBatch> {
        let vectors = FixedSizeListArray::new(
            PrimitiveArray::new(Buffer::from(values), Validity::NonNullable).into_array(),
            self.dimension,
            Validity::NonNullable,
            rows.len(),
        );
        SourceBatch::try_new(
            &self.snapshot,
            rows,
            StructArray::try_from_iter([(self.field.as_str(), vectors.into_array())])?.into_array(),
        )
    }
}

#[async_trait]
impl IndexSource for PhysicalSource {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn scan(
        &self,
        files: &[u64],
        fields: &[String],
    ) -> VortexResult<BoxStream<'static, VortexResult<SourceBatch>>> {
        self.validate_projection(fields)?;
        let mut streams = Vec::new();
        for id in files {
            let source = self
                .files
                .get(id)
                .ok_or_else(|| vortex_err!("Unknown physical file ID"))?;
            let adapter = self.adapter();
            let stream = source.scan(&[*id], fields)?.map(move |batch| {
                let batch = batch?;
                let values = adapter.vectors(&batch)?;
                adapter.batch(batch.rows, values)
            });
            streams.push(Box::pin(stream) as BoxStream<'static, VortexResult<SourceBatch>>);
        }
        Ok(Box::pin(futures::stream::iter(streams).flatten()))
    }

    async fn take(&self, rows: &[RowAddress], fields: &[String]) -> VortexResult<SourceBatch> {
        self.validate_projection(fields)?;
        let dimension = self.capture.dimension as usize;
        let mut groups: BTreeMap<u64, Vec<(usize, RowAddress)>> = BTreeMap::new();
        for (rank, row) in rows.iter().enumerate() {
            self.snapshot.validate_row(*row)?;
            groups.entry(row.file_id).or_default().push((rank, *row));
        }
        let mut values = vec![0.0; rows.len() * dimension];
        for (id, group) in groups {
            let selected: Vec<_> = group.iter().map(|(_, row)| *row).collect();
            let source = self
                .files
                .get(&id)
                .ok_or_else(|| vortex_err!("Unknown physical file ID"))?;
            let taken = source.take(&selected, fields).await?;
            if taken.rows != selected {
                vortex_bail!("Physical take changed row order");
            }
            let vectors = self.adapter().vectors(&taken)?;
            for (offset, (rank, _)) in group.iter().enumerate() {
                values[rank * dimension..(rank + 1) * dimension]
                    .copy_from_slice(&vectors[offset * dimension..(offset + 1) * dimension]);
            }
        }
        self.adapter().batch(rows.to_vec(), values)
    }
}
