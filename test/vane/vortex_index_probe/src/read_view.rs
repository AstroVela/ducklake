//! Explicit retained views for the local lifecycle proof.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::io::BufRead;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::session::VortexSession;
use vortex_index::Index;
use vortex_index::IndexProvider;
use vortex_index::IndexSource;
use vortex_index::RowAddress;
use vortex_index::Snapshot;
use vortex_index::store::LocalGeneration;
use vortex_index::store::LocalIndexStore;

use crate::Hit;
use crate::LIMITS;
use crate::Request;
use crate::mixed::MixedRequest;
use crate::mixed::MixedView;
use crate::mixed::RowRef;
use crate::provider;
use crate::query_hits;
use crate::reference_for;
use crate::source::BusinessRow;
use crate::source::PhysicalSource;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    max_views: usize,
    max_source_bytes: usize,
    max_artifact_bytes: u64,
    max_inline_rows: usize,
    max_inline_bytes: usize,
}

pub struct ViewBudget {
    pub source_bytes: usize,
    pub artifact_bytes: u64,
    pub inline_rows: usize,
    pub inline_bytes: usize,
}

pub struct IndexLease {
    // Close native handles before releasing their private directories.
    pub index: Arc<dyn Index>,
    _scratch: TempDir,
    pub artifact_bytes: u64,
}

#[derive(Serialize)]
struct ViewHit {
    #[serde(flatten)]
    hit: Hit,
    row: BusinessRow,
}

impl IndexLease {
    pub async fn open(
        reference: &Path,
        generation: &LocalGeneration,
        snapshot: &Snapshot,
        leases: &Path,
        artifact_budget: u64,
    ) -> VortexResult<Self> {
        let root = reference
            .parent()
            .ok_or_else(|| vortex_err!("Reference parent required"))?;
        let pending = LocalIndexStore::prepare_open(root, generation, snapshot, LIMITS)?;
        let artifact_bytes =
            pending
                .metadata()
                .artifacts
                .iter()
                .try_fold(0u64, |total, artifact| {
                    total
                        .checked_add(artifact.size)
                        .ok_or_else(|| vortex_err!("Artifact byte count overflow"))
                })?;
        if artifact_bytes > artifact_budget {
            vortex_bail!("Retained artifact byte budget exceeded");
        }
        let scratch = tempfile::Builder::new()
            .prefix(".read-view-")
            .tempdir_in(leases)?;
        let (store, metadata) = pending.materialize(scratch.path(), artifact_budget)?;
        let index = provider(scratch.path())?.open(&metadata, store).await?;
        Ok(Self {
            index,
            _scratch: scratch,
            artifact_bytes,
        })
    }
}

struct ReadView {
    lease: IndexLease,
    source: Arc<PhysicalSource>,
    visible: BTreeSet<RowAddress>,
}

impl ReadView {
    async fn open(
        capture: &Path,
        reference: &Path,
        leases: &Path,
        source_budget: usize,
        artifact_budget: u64,
        session: VortexSession,
    ) -> VortexResult<Self> {
        if !capture.is_absolute() || !reference.is_absolute() {
            vortex_bail!("Retained view paths must be absolute");
        }
        let source = Arc::new(
            PhysicalSource::open_with_budget(&fs::read(capture)?, session, source_budget).await?,
        );
        if source.capture.business_fields.is_none() {
            vortex_bail!("Retained views require captured business field IDs");
        }
        let descriptor = reference_for(&source, reference)?;
        let lease = IndexLease::open(
            reference,
            &descriptor.generation,
            source.snapshot(),
            leases,
            artifact_budget,
        )
        .await?;
        let visible = source.capture.visible.iter().copied().collect();
        Ok(Self {
            lease,
            source,
            visible,
        })
    }

    fn check_digest(&self, digest: &str) -> VortexResult<()> {
        if digest != self.source.digest {
            vortex_bail!("Read view capture mismatch");
        }
        Ok(())
    }

    async fn fetch(&self, rows: &[RowAddress]) -> VortexResult<Vec<BusinessRow>> {
        if rows.len() > 4096 {
            vortex_bail!("Retained fetch exceeds its row budget");
        }
        if rows.iter().any(|row| !self.visible.contains(row)) {
            vortex_bail!("Retained fetch requires visible addresses from this read view");
        }
        self.source.take_business(rows).await
    }

    async fn search(&self, request: Request, session: VortexSession) -> VortexResult<Vec<ViewHit>> {
        let hits = query_hits(&self.source, self.lease.index.as_ref(), request, session).await?;
        let rows: Vec<_> = hits
            .iter()
            .map(|hit| RowAddress {
                file_id: hit.file_id,
                row_offset: hit.row_offset,
            })
            .collect();
        let fetched = self.fetch(&rows).await?;
        if hits
            .iter()
            .zip(&fetched)
            .any(|(hit, row)| hit.embedding != row.embedding)
        {
            vortex_bail!("Pinned vector and business take disagree");
        }
        Ok(hits
            .into_iter()
            .zip(fetched)
            .map(|(hit, row)| ViewHit { hit, row })
            .collect())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    id: u64,
    command: Command,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Open {
        view: String,
        capture: PathBuf,
        reference: PathBuf,
    },
    Search {
        view: String,
        capture_digest: String,
        request: Request,
    },
    Fetch {
        view: String,
        capture_digest: String,
        rows: Vec<RowAddress>,
    },
    OpenMixed {
        view: String,
        capture: PathBuf,
        reference: PathBuf,
    },
    SearchMixed {
        view: String,
        capture_digest: String,
        request: MixedRequest,
    },
    FetchMixed {
        view: String,
        capture_digest: String,
        rows: Vec<RowRef>,
    },
    Close {
        view: String,
    },
    Stats,
    Shutdown,
}

enum RetainedView {
    Snapshot(ReadView),
    Mixed(MixedView),
}

impl RetainedView {
    fn source(&self) -> &PhysicalSource {
        match self {
            Self::Snapshot(view) => &view.source,
            Self::Mixed(view) => &view.source,
        }
    }

    fn artifact_bytes(&self) -> u64 {
        match self {
            Self::Snapshot(view) => view.lease.artifact_bytes,
            Self::Mixed(view) => view.lease.artifact_bytes,
        }
    }

    fn inline_rows(&self) -> usize {
        match self {
            Self::Snapshot(_) => 0,
            Self::Mixed(view) => view.inline_rows(),
        }
    }

    fn inline_bytes(&self) -> usize {
        match self {
            Self::Snapshot(_) => 0,
            Self::Mixed(view) => view.inline_bytes,
        }
    }
}

struct Registry {
    views: BTreeMap<String, RetainedView>,
    leases: PathBuf,
    limits: Limits,
    session: VortexSession,
}

impl Registry {
    fn source_bytes(&self) -> usize {
        self.views
            .values()
            .map(|view| view.source().pinned_bytes())
            .sum()
    }

    fn artifact_bytes(&self) -> u64 {
        self.views.values().map(|view| view.artifact_bytes()).sum()
    }

    fn inline_rows(&self) -> usize {
        self.views.values().map(|view| view.inline_rows()).sum()
    }

    fn inline_bytes(&self) -> usize {
        self.views.values().map(|view| view.inline_bytes()).sum()
    }

    fn stats(&self) -> VortexResult<Value> {
        Ok(
            json!({"active_views":self.views.len(), "source_bytes":self.source_bytes(),
            "artifact_bytes":self.artifact_bytes(), "inline_rows":self.inline_rows(),
            "inline_bytes":self.inline_bytes(), "lease_directories":fs::read_dir(&self.leases)?.count()}),
        )
    }

    fn view(&self, name: &str, digest: &str) -> VortexResult<&ReadView> {
        let view = self
            .views
            .get(name)
            .ok_or_else(|| vortex_err!("Unknown read view"))?;
        let RetainedView::Snapshot(view) = view else {
            vortex_bail!("Read view requires mixed commands");
        };
        view.check_digest(digest)?;
        Ok(view)
    }

    fn mixed_view(&self, name: &str, digest: &str) -> VortexResult<&MixedView> {
        let view = self
            .views
            .get(name)
            .ok_or_else(|| vortex_err!("Unknown read view"))?;
        let RetainedView::Mixed(view) = view else {
            vortex_bail!("Read view requires snapshot commands");
        };
        view.check_digest(digest)?;
        Ok(view)
    }

    async fn execute(&mut self, command: Command) -> VortexResult<Value> {
        let mixed = matches!(&command, Command::OpenMixed { .. });
        match command {
            Command::Open {
                view,
                capture,
                reference,
            }
            | Command::OpenMixed {
                view,
                capture,
                reference,
            } => {
                if view.is_empty() || self.views.contains_key(&view) {
                    vortex_bail!("Read view identity is empty or already open");
                }
                if self.views.len() >= self.limits.max_views {
                    vortex_bail!("Read view count budget exceeded");
                }
                let source_budget = self
                    .limits
                    .max_source_bytes
                    .saturating_sub(self.source_bytes());
                let artifact_budget = self
                    .limits
                    .max_artifact_bytes
                    .saturating_sub(self.artifact_bytes());
                let opened = if mixed {
                    RetainedView::Mixed(
                        MixedView::open(
                            &capture,
                            &reference,
                            &self.leases,
                            ViewBudget {
                                source_bytes: source_budget,
                                artifact_bytes: artifact_budget,
                                inline_rows: self
                                    .limits
                                    .max_inline_rows
                                    .saturating_sub(self.inline_rows()),
                                inline_bytes: self
                                    .limits
                                    .max_inline_bytes
                                    .saturating_sub(self.inline_bytes()),
                            },
                            self.session.clone(),
                        )
                        .await?,
                    )
                } else {
                    RetainedView::Snapshot(
                        ReadView::open(
                            &capture,
                            &reference,
                            &self.leases,
                            source_budget,
                            artifact_budget,
                            self.session.clone(),
                        )
                        .await?,
                    )
                };
                let digest = match &opened {
                    RetainedView::Snapshot(view) => &view.source.digest,
                    RetainedView::Mixed(view) => &view.digest,
                };
                let result = json!({"capture_digest":digest,
                    "snapshot_id":opened.source().capture.snapshot_id,
                    "source_bytes":opened.source().pinned_bytes(), "artifact_bytes":opened.artifact_bytes(),
                    "inline_rows":opened.inline_rows(), "inline_bytes":opened.inline_bytes()});
                self.views.insert(view, opened);
                Ok(result)
            }
            Command::Search {
                view,
                capture_digest,
                request,
            } => serde_json::to_value(
                self.view(&view, &capture_digest)?
                    .search(request, self.session.clone())
                    .await?,
            )
            .map_err(|error| vortex_err!("{error}")),
            Command::Fetch {
                view,
                capture_digest,
                rows,
            } => serde_json::to_value(self.view(&view, &capture_digest)?.fetch(&rows).await?)
                .map_err(|error| vortex_err!("{error}")),
            Command::SearchMixed {
                view,
                capture_digest,
                request,
            } => serde_json::to_value(
                self.mixed_view(&view, &capture_digest)?
                    .search(request, self.session.clone())
                    .await?,
            )
            .map_err(|error| vortex_err!("{error}")),
            Command::FetchMixed {
                view,
                capture_digest,
                rows,
            } => serde_json::to_value(
                self.mixed_view(&view, &capture_digest)?
                    .fetch(&rows)
                    .await?,
            )
            .map_err(|error| vortex_err!("{error}")),
            Command::Close { view } => {
                drop(
                    self.views
                        .remove(&view)
                        .ok_or_else(|| vortex_err!("Unknown read view"))?,
                );
                self.stats()
            }
            Command::Stats => self.stats(),
            Command::Shutdown => {
                self.views.clear();
                self.stats()
            }
        }
    }
}

pub async fn serve(root: &Path, limits: &str, session: VortexSession) -> VortexResult<()> {
    if !root.is_absolute() {
        vortex_bail!("Read view worker root must be absolute");
    }
    let limits: Limits = serde_json::from_str(limits).map_err(|error| vortex_err!("{error}"))?;
    if limits.max_views == 0 || limits.max_source_bytes == 0 || limits.max_artifact_bytes == 0 {
        vortex_bail!("Read view budgets must be nonzero");
    }
    fs::create_dir(root)?;
    let leases = root.join("leases");
    let responses = root.join("responses");
    fs::create_dir(&leases)?;
    fs::create_dir(&responses)?;
    let mut registry = Registry {
        views: BTreeMap::new(),
        leases,
        limits,
        session,
    };
    for line in io::stdin().lock().lines() {
        let envelope: Envelope =
            serde_json::from_str(&line?).map_err(|error| vortex_err!("{error}"))?;
        let stop = matches!(envelope.command, Command::Shutdown);
        let result = match registry.execute(envelope.command).await {
            Ok(result) => json!({"ok":true, "result":result}),
            Err(error) => json!({"ok":false, "error":error.to_string()}),
        };
        let mut response = tempfile::NamedTempFile::new_in(&responses)?;
        serde_json::to_writer(&mut response, &result).map_err(|error| vortex_err!("{error}"))?;
        response.flush()?;
        response
            .persist_noclobber(responses.join(format!("{}.json", envelope.id)))
            .map_err(|error| vortex_err!("{error}"))?;
        if stop {
            break;
        }
    }
    Ok(())
}
