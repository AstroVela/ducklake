//! Consume only the explicitly selected native, self-contained admission format.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::Deserialize;
use tempfile::TempDir;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;

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
