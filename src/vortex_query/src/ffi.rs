//! Synchronous, owned result boundary; no Rust unwind crosses C++.
use std::ffi::{CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::slice;
use std::str;
use std::{cell::RefCell, collections::BTreeMap, time::Instant};

use crate::{
    lease::ViewBudget,
    mixed::{MixedHit, MixedView},
};
use vortex::{
    VortexSessionDefault,
    buffer::ByteBuffer,
    error::{VortexResult, vortex_bail, vortex_err},
    io::{runtime::single::block_on, session::RuntimeSessionExt},
    session::VortexSession,
};

#[repr(C)]
pub struct Source {
    file_id: u64,
    data: *const u8,
    size: u64,
}

#[repr(C)]
pub struct Input {
    admission: *const u8,
    admission_len: u64,
    sources: *const Source,
    source_count: u64,
    data_path: *const u8,
    data_path_len: u64,
    index_name: *const u8,
    index_name_len: u64,
    query: *const f32,
    dimension: u64,
    k: u64,
    category: *const u8,
    category_len: u64,
    filter_category: u8,
    control: extern "C" fn(*mut c_void, *const u8, u64) -> u8,
    context: *mut c_void,
}

#[repr(C)]
pub struct Row {
    id: i64,
    distance: f32,
    embedding: *const f32,
    dimension: u64,
    category: *const u8,
    category_len: u64,
    payload: *const u8,
    payload_len: u64,
    id_null: u8,
    category_null: u8,
    payload_null: u8,
}

pub struct Output {
    hits: Vec<MixedHit>,
    diagnostics: CString,
    error: Option<CString>,
}

impl Output {
    fn error(error: String) -> Self {
        Self {
            hits: vec![],
            diagnostics: CString::default(),
            error: Some(CString::new(error.replace('\0', "\\0")).expect("NUL escaped")),
        }
    }
}

// Descriptor and data buffers are borrowed only while this bounded copy runs.
pub(crate) unsafe fn copy_sources(
    pointer: *const Source,
    count: u64,
) -> VortexResult<BTreeMap<u64, ByteBuffer>> {
    if pointer.is_null() || !(1..=100_000).contains(&count) {
        vortex_bail!("Invalid native binary source inventory");
    }
    let sources = unsafe { slice::from_raw_parts(pointer, count as usize) };
    let mut buffers = BTreeMap::new();
    let mut remaining = 64 * 1024 * 1024;
    for source in sources {
        if source.data.is_null()
            || source.size == 0
            || source.size > remaining as u64
            || buffers.contains_key(&source.file_id)
        {
            vortex_bail!("Invalid, duplicate or over-budget native source buffer");
        }
        let bytes = unsafe { slice::from_raw_parts(source.data, source.size as usize) };
        buffers.insert(source.file_id, ByteBuffer::copy_from(bytes));
        remaining -= bytes.len();
    }
    Ok(buffers)
}

// The caller keeps metadata/callbacks alive until return and source bytes until source_pinned.
unsafe fn execute(input: &Input) -> VortexResult<Output> {
    if input.admission.is_null()
        || input.admission_len == 0
        || input.admission_len > 32 * 1024 * 1024
        || input.data_path.is_null()
        || !(1..=65536).contains(&input.data_path_len)
        || input.index_name.is_null()
        || !(1..=128).contains(&input.index_name_len)
        || input.query.is_null()
        || !(1..=4096).contains(&input.dimension)
        || !(1..=64).contains(&input.k)
        || input.filter_category > 1
        || input.category_len > 64 * 1024
        || (input.filter_category == 1 && input.category.is_null())
    {
        vortex_bail!("Invalid native Vortex SQL query input");
    }
    let bytes = unsafe { slice::from_raw_parts(input.admission, input.admission_len as usize) };
    let name = str::from_utf8(unsafe {
        slice::from_raw_parts(input.index_name, input.index_name_len as usize)
    })
    .map_err(|error| vortex_err!("Invalid UTF-8 index name: {error}"))?;
    let query = unsafe { slice::from_raw_parts(input.query, input.dimension as usize) }.to_vec();
    let category = if input.filter_category == 1 {
        Some(
            str::from_utf8(unsafe {
                slice::from_raw_parts(input.category, input.category_len as usize)
            })
            .map_err(|error| vortex_err!("Invalid UTF-8 category: {error}"))?,
        )
    } else {
        None
    };
    let started = Instant::now();
    let last = RefCell::new(started);
    let phases = RefCell::new(BTreeMap::<&str, u64>::new());
    let check = |stage: &str| {
        let phase = match stage {
            "before_open" => Some("setup_us"),
            "sources_staged" => Some("metadata_prepare_us"),
            "source_pinned" => Some("source_open_us"),
            "after_open" => Some("index_open_us"),
            "before_ann" => Some("filter_us"),
            "after_ann" => Some("ann_us"),
            "before_scan" => Some("rerank_us"),
            "inline_top_k" => Some("file_scan_us"),
            "before_fetch" => Some("inline_merge_us"),
            "after_fetch" => Some("ranked_take_us"),
            _ => None,
        };
        if let Some(phase) = phase {
            let now = Instant::now();
            phases
                .borrow_mut()
                .insert(phase, now.duration_since(*last.borrow()).as_micros() as u64);
            *last.borrow_mut() = now;
        }

        if (input.control)(input.context, stage.as_ptr(), stage.len() as u64) != 0 {
            vortex_bail!("Native Vortex SQL query interrupted");
        }
        Ok(())
    };
    check("start")?;
    let data_path = str::from_utf8(unsafe {
        slice::from_raw_parts(input.data_path, input.data_path_len as usize)
    })
    .map_err(|error| vortex_err!("Invalid UTF-8 data path: {error}"))?;
    let data_path = std::path::Path::new(data_path).canonicalize()?;
    let scratch_root = std::env::temp_dir().canonicalize()?;
    if scratch_root.starts_with(data_path) {
        vortex_bail!("Vortex SQL scratch directory must be outside the managed data path");
    }
    let scratch = tempfile::Builder::new()
        .prefix(".ducklake-vortex-query-")
        .tempdir_in(scratch_root)?;
    block_on(|handle| async {
        let session = VortexSession::default().with_handle(handle);
        let buffers = unsafe { copy_sources(input.sources, input.source_count) }?;
        let handoff_bytes = buffers.values().map(|bytes| bytes.len()).sum::<usize>();
        let source_count = buffers.len();
        let view = MixedView::from_binary(
            bytes,
            name,
            buffers,
            scratch.path(),
            ViewBudget {
                source_bytes: 64 * 1024 * 1024,
                artifact_bytes: 64 * 1024 * 1024,
                inline_rows: 100_000,
                inline_bytes: 4 * 1024 * 1024,
            },
            session.clone(),
            &check,
        )
        .await?;
        let result = view
            .search_category(query, input.k as usize, category, session, &check)
            .await?;
        let diagnostics = serde_json::to_vec(&serde_json::json!({"routes":result.routes,
            "index_revision":view.catalog_revision,"generation":view.generation(),"generations":view.generations(),
            "snapshot_id":view.source.capture.snapshot_id, "cost":{"phases_us":phases.borrow().clone(),
            "projection_rows":view.source.projection_counts(),"source_bytes":view.source.pinned_bytes(),
            "artifact_bytes":view.artifact_bytes(),"binary_handoff_bytes":handoff_bytes,"source_buffer_count":source_count,"source_staging_bytes":0,"elapsed_us":started.elapsed().as_micros() as u64}}))
            .map_err(|e| vortex_err!("{e}"))?;
        Ok(Output {
            hits: result.hits,
            diagnostics: CString::new(diagnostics).map_err(|e| vortex_err!("{e}"))?,
            error: None,
        })
    })
}

/// # Safety
/// Metadata, descriptors and callbacks must remain valid until return. Source
/// bytes must be readable until the source_pinned callback or an earlier return.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ducklake_vortex_query(input: &Input) -> *mut Output {
    let result = catch_unwind(AssertUnwindSafe(|| unsafe { execute(input) }));
    let output = match result {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => Output::error(error.to_string()),
        Err(_) => Output::error("Native Vortex query panicked".to_owned()),
    };
    Box::into_raw(Box::new(output))
}

/// # Safety
/// Handle must be a live result returned by ducklake_vortex_query.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ducklake_vortex_query_error(output: &Output) -> *const c_char {
    output
        .error
        .as_ref()
        .map_or(ptr::null(), |error| error.as_ptr())
}

/// # Safety
/// Handle must be a live result returned by ducklake_vortex_query.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ducklake_vortex_query_diagnostics(output: &Output) -> *const c_char {
    output.diagnostics.as_ptr()
}

/// # Safety
/// Handle must be a live result returned by ducklake_vortex_query.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ducklake_vortex_query_count(output: &Output) -> u64 {
    output.hits.len() as u64
}

/// # Safety
/// Handle and row pointer must be valid; returned buffers live until free.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ducklake_vortex_query_row(
    output: &Output,
    index: u64,
    row: &mut Row,
) -> u8 {
    let Some(hit) = output.hits.get(index as usize) else {
        return 1;
    };
    let business = &hit.row;
    let (category, category_len) = business
        .category
        .as_ref()
        .map_or((ptr::null(), 0), |s| (s.as_ptr(), s.len() as u64));
    let (payload, payload_len) = business
        .payload
        .as_ref()
        .map_or((ptr::null(), 0), |s| (s.as_ptr(), s.len() as u64));
    *row = Row {
        id: business.id.unwrap_or_default(),
        distance: hit.distance,
        embedding: business.embedding.as_ptr(),
        dimension: business.embedding.len() as u64,
        category,
        category_len,
        payload,
        payload_len,
        id_null: u8::from(business.id.is_none()),
        category_null: u8::from(business.category.is_none()),
        payload_null: u8::from(business.payload.is_none()),
    };
    0
}

/// # Safety
/// Handle must be live and must be freed exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ducklake_vortex_query_free(output: *mut Output) {
    unsafe {
        drop(Box::from_raw(output));
    }
}
