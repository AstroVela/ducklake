//! Native file-only construction through the same owned buffer and SPFresh core.
use std::ffi::{CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::{path::Path, ptr, slice, str, sync::Arc};

use crate::{
    ffi::{Source, copy_sources},
    file_index,
    source::{Capture, PhysicalSource},
};
use serde::Deserialize;
use vortex::{
    VortexSessionDefault,
    error::{VortexResult, vortex_bail, vortex_err},
    io::{runtime::single::block_on, session::RuntimeSessionExt},
    session::VortexSession,
};

#[repr(C)]
pub struct BuildInput {
    admission: *const u8,
    admission_len: u64,
    sources: *const Source,
    source_count: u64,
    data_path: *const u8,
    data_path_len: u64,
    reference_path: *const u8,
    reference_path_len: u64,
    control: extern "C" fn(*mut c_void, *const u8, u64) -> u8,
    context: *mut c_void,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BuildAdmission {
    format_version: u32,
    capture: Capture,
}

// All metadata/descriptors remain readable until return; bytes until source_pinned.
unsafe fn execute(input: &BuildInput) -> VortexResult<()> {
    if input.admission.is_null()
        || !(1..=32 * 1024 * 1024).contains(&input.admission_len)
        || input.data_path.is_null()
        || !(1..=65536).contains(&input.data_path_len)
        || input.reference_path.is_null()
        || !(1..=65536).contains(&input.reference_path_len)
    {
        vortex_bail!("Invalid native Vortex build input");
    }
    let check = |stage: &str| {
        if (input.control)(input.context, stage.as_ptr(), stage.len() as u64) != 0 {
            vortex_bail!("Native Vortex build interrupted");
        }
        Ok(())
    };
    check("build_input")?;
    let bytes = unsafe { slice::from_raw_parts(input.admission, input.admission_len as usize) };
    let admission: BuildAdmission =
        serde_json::from_slice(bytes).map_err(|e| vortex_err!("{e}"))?;
    if admission.format_version != 1 || !admission.capture.visible.is_empty() {
        vortex_bail!("Build admission requires an unfiltered physical-file capture");
    }
    let data_path = str::from_utf8(unsafe {
        slice::from_raw_parts(input.data_path, input.data_path_len as usize)
    })
    .map_err(|e| vortex_err!("{e}"))?;
    let reference = str::from_utf8(unsafe {
        slice::from_raw_parts(input.reference_path, input.reference_path_len as usize)
    })
    .map_err(|e| vortex_err!("{e}"))?;
    let reference = Path::new(reference);
    let root = reference
        .parent()
        .ok_or_else(|| vortex_err!("Build reference parent required"))?;
    let canonical = root.canonicalize()?;
    if !reference.is_absolute()
        || root != canonical
        || canonical.starts_with(Path::new(data_path).canonicalize()?)
        || reference.exists()
        || reference.file_name().is_none()
    {
        vortex_bail!("Build reference must be new, canonical and outside the managed data path");
    }
    let capture = serde_json::to_vec(&admission.capture).map_err(|e| vortex_err!("{e}"))?;
    block_on(|handle| async {
        let session = VortexSession::default().with_handle(handle);
        let buffers = unsafe { copy_sources(input.sources, input.source_count) }?;
        let source = Arc::new(
            PhysicalSource::open_buffers(&capture, session.clone(), 64 * 1024 * 1024, buffers)
                .await?,
        );
        check("source_pinned")?;
        file_index::build_checked(source, reference, session, &check).await
    })
}

/// # Safety
/// Metadata, descriptors and callback must live until return. Source bytes must
/// remain readable until source_pinned or earlier return. NULL means success.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ducklake_vortex_build(input: &BuildInput) -> *mut c_char {
    let error = match catch_unwind(AssertUnwindSafe(|| unsafe { execute(input) })) {
        Ok(Ok(())) => return ptr::null_mut(),
        Ok(Err(error)) => error.to_string(),
        Err(_) => "Native Vortex build panicked".to_owned(),
    };
    CString::new(error.replace('\0', "\\0"))
        .expect("NUL escaped")
        .into_raw()
}

/// # Safety
/// Error must be a live non-NULL result from ducklake_vortex_build, freed once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ducklake_vortex_build_error_free(error: *mut c_char) {
    unsafe {
        drop(CString::from_raw(error));
    }
}
