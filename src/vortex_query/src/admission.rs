//! Consume only the explicitly selected native, self-contained admission format.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use tempfile::TempDir;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;

use crate::file_index::Reference;
use crate::mixed::MixedCapture;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EncodedSource {
    file_id: u64,
    hex: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    format_version: u32,
    capture: MixedCapture,
    sources: Vec<EncodedSource>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogIndex {
    pub name: String,
    pub revision: u64,
    pub root: PathBuf,
    pub reference: SegmentManifest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSegment {
    pub root: PathBuf,
    pub reference: Reference,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentManifest {
    pub format_version: u32,
    pub table_uuid: String,
    pub field_id: u32,
    pub dimension: u32,
    pub root: PathBuf,
    pub publication: String,
    pub segments: Vec<CatalogSegment>,
}

impl CatalogIndex {
    fn validate(&self, name: &str, capture: &MixedCapture) -> VortexResult<()> {
        if self.name != name
            || name.is_empty()
            || self.revision == 0
            || self.reference.format_version != 3
            || self.reference.table_uuid != capture.file_capture.table_uuid
            || self.reference.field_id != capture.file_capture.field_id
            || self.reference.dimension != capture.file_capture.dimension
            || !(1..=4096).contains(&self.reference.dimension)
            || self.reference.publication.len() != 36
            || !self
                .reference
                .publication
                .bytes()
                .enumerate()
                .all(|(i, value)| {
                    if [8, 13, 18, 23].contains(&i) {
                        value == b'-'
                    } else {
                        value.is_ascii_digit() || (b'a'..=b'f').contains(&value)
                    }
                })
            || !self.root.is_absolute()
            || self.reference.root != self.root
            || self.reference.segments.len() > 3
            || self
                .reference
                .segments
                .iter()
                .any(|segment| !segment.root.is_absolute())
            || self
                .reference
                .segments
                .last()
                .is_some_and(|segment| segment.root != self.root)
        {
            vortex_bail!(
                "Catalogued segment manifest name, revision, version, roots or count mismatch"
            );
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogAdmission {
    format_version: u32,
    capture: MixedCapture,
    sources: Vec<EncodedSource>,
    index: CatalogIndex,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BinaryAdmission {
    format_version: u32,
    capture: MixedCapture,
    index: CatalogIndex,
}

pub fn prepare_binary(bytes: &[u8], name: &str) -> VortexResult<(MixedCapture, CatalogIndex)> {
    if bytes.len() > 32 * 1024 * 1024 {
        vortex_bail!("Binary admission metadata exceeds its byte budget");
    }
    let admission: BinaryAdmission =
        serde_json::from_slice(bytes).map_err(|error| vortex_err!("{error}"))?;
    if admission.format_version != 1 {
        vortex_bail!("Binary admission version mismatch");
    }
    admission.index.validate(name, &admission.capture)?;
    Ok((admission.capture, admission.index))
}

pub struct Pins {
    pub files: BTreeMap<u64, String>,
    _scratch: TempDir,
}

pub struct Prepared {
    pub capture: MixedCapture,
    pub pins: Pins,
}

pub fn prepare(bytes: &[u8], source_budget: usize, scratch_root: &Path) -> VortexResult<Prepared> {
    if bytes.len() > 160 * 1024 * 1024 {
        vortex_bail!("Native admission exceeds its byte budget");
    }
    let admission: Admission =
        serde_json::from_slice(bytes).map_err(|error| vortex_err!("{error}"))?;
    materialize(admission, source_budget, scratch_root)
}

pub fn prepare_catalogued(
    bytes: &[u8],
    name: &str,
    source_budget: usize,
    scratch_root: &Path,
) -> VortexResult<(Prepared, CatalogIndex)> {
    if bytes.len() > 160 * 1024 * 1024 {
        vortex_bail!("Native admission exceeds its byte budget");
    }
    let admission: CatalogAdmission =
        serde_json::from_slice(bytes).map_err(|error| vortex_err!("{error}"))?;
    admission.index.validate(name, &admission.capture)?;
    let prepared = materialize(
        Admission {
            format_version: admission.format_version,
            capture: admission.capture,
            sources: admission.sources,
        },
        source_budget,
        scratch_root,
    )?;
    Ok((prepared, admission.index))
}

fn materialize(
    admission: Admission,
    source_budget: usize,
    scratch_root: &Path,
) -> VortexResult<Prepared> {
    if admission.format_version != 1
        || admission.sources.len() != admission.capture.file_capture.files.len()
    {
        vortex_bail!("Native admission version or source inventory mismatch");
    }
    let scratch = tempfile::Builder::new()
        .prefix(".native-source-")
        .tempdir_in(scratch_root)?;
    let mut files = BTreeMap::new();
    let mut remaining = source_budget.min(64 * 1024 * 1024);
    for source in admission.sources {
        if !admission
            .capture
            .file_capture
            .files
            .iter()
            .any(|file| file.id == source.file_id)
            || files.contains_key(&source.file_id)
        {
            vortex_bail!("Native admission has an unknown or duplicate source file");
        }
        if source.hex.len() % 2 != 0 || source.hex.len() / 2 > remaining {
            vortex_bail!("Native admission source byte budget or hex length mismatch");
        }
        let nibble = |value: u8| -> VortexResult<u8> {
            match value {
                b'0'..=b'9' => Ok(value - b'0'),
                b'a'..=b'f' => Ok(value - b'a' + 10),
                _ => Err(vortex_err!(
                    "Native source requires lowercase hexadecimal bytes"
                )),
            }
        };
        let decoded = source
            .hex
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| Ok(nibble(pair[0])? * 16 + nibble(pair[1])?))
            .collect::<VortexResult<Vec<_>>>()?;
        remaining -= decoded.len();
        let path = scratch
            .path()
            .join(format!("source-{}.vortex", source.file_id));
        fs::write(&path, decoded)?;
        files.insert(source.file_id, path.to_string_lossy().into_owned());
    }
    Ok(Prepared {
        capture: admission.capture,
        pins: Pins {
            files,
            _scratch: scratch,
        },
    })
}
