//! Registered native-query scratch; only explicit, authenticated collection reclaims crashed owners.
use crate::LIMITS;
use rustix::fs::{self as posix, AtFlags, FlockOperation, Mode, OFlags, Stat};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use vortex::error::{VortexResult, vortex_bail, vortex_err};
use vortex_index::file::file_version;

const PRIVATE: Mode = Mode::from_bits_truncate(0o700);
const FILE_MODE: Mode = Mode::from_bits_truncate(0o600);
const DIRECTORY: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const FILE_READ: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    device: u64,
    inode: u64,
}

fn identity(stat: &Stat) -> Identity {
    Identity {
        device: stat.st_dev as u64,
        inode: stat.st_ino as u64,
    }
}
fn uid() -> u32 {
    rustix::process::geteuid().as_raw()
}
fn is_dir(stat: &Stat) -> bool {
    stat.st_mode & 0o170000 == 0o040000
}
fn validate_stat(stat: &Stat, directory: bool, private: bool) -> VortexResult<()> {
    if stat.st_uid != uid()
        || is_dir(stat) != directory
        // SDK payload permissions inherit the caller's umask. The registered 0700
        // root and payload isolate them; protocol controls remain strictly private.
        || stat.st_mode & 0o002 != 0
        || (!directory && (stat.st_mode & 0o170000 != 0o100000 || stat.st_nlink != 1))
        || (private && stat.st_mode & 0o077 != 0)
    {
        vortex_bail!("Scratch ownership, type, permissions or hard-link identity mismatch");
    }
    Ok(())
}
fn same(fd: &OwnedFd, expected: Identity, directory: bool, private: bool) -> VortexResult<()> {
    let stat = posix::fstat(fd).map_err(std::io::Error::from)?;
    validate_stat(&stat, directory, private)?;
    if identity(&stat) != expected {
        vortex_bail!("Scratch filesystem identity changed");
    }
    Ok(())
}
fn child(dir: &OwnedFd, name: &str) -> VortexResult<OwnedFd> {
    let fd = posix::openat(dir, name, DIRECTORY, Mode::empty()).map_err(std::io::Error::from)?;
    validate_stat(
        &posix::fstat(&fd).map_err(std::io::Error::from)?,
        true,
        false,
    )?;
    Ok(fd)
}
fn exists(dir: &OwnedFd, name: &str) -> VortexResult<Option<Stat>> {
    match posix::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(stat)),
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(std::io::Error::from(error).into()),
    }
}
fn names(dir: &OwnedFd) -> VortexResult<Vec<String>> {
    let mut result = vec![];
    let mut bytes = 0;
    for entry in posix::Dir::read_from(dir).map_err(std::io::Error::from)? {
        let entry = entry.map_err(std::io::Error::from)?;
        let value = entry.file_name().to_str().map_err(|e| vortex_err!("{e}"))?;
        if value == "." || value == ".." {
            continue;
        }
        bytes += value.len() + 1;
        if bytes > LIMITS.max_manifest_bytes {
            vortex_bail!("Scratch directory inventory exceeds manifest budget");
        }
        result.push(value.to_owned());
    }
    result.sort();
    Ok(result)
}
fn read_json<T: serde::de::DeserializeOwned>(dir: &OwnedFd, name: &str) -> VortexResult<T> {
    let fd = posix::openat(dir, name, FILE_READ, Mode::empty()).map_err(std::io::Error::from)?;
    let stat = posix::fstat(&fd).map_err(std::io::Error::from)?;
    validate_stat(&stat, false, true)?;
    if stat.st_size < 0 || stat.st_size as usize > LIMITS.max_manifest_bytes {
        vortex_bail!("Scratch record exceeds manifest budget");
    }
    let mut bytes = vec![];
    File::from(fd)
        .take(LIMITS.max_manifest_bytes as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > LIMITS.max_manifest_bytes {
        vortex_bail!("Scratch record exceeds manifest budget");
    }
    serde_json::from_slice(&bytes).map_err(|e| vortex_err!("Invalid scratch record: {e}"))
}
fn private_file(dir: &OwnedFd, name: &str, create: bool) -> VortexResult<File> {
    let flags = OFlags::RDWR
        | OFlags::NOFOLLOW
        | OFlags::CLOEXEC
        | if create {
            OFlags::CREATE
        } else {
            OFlags::empty()
        };
    let fd = posix::openat(dir, name, flags, FILE_MODE).map_err(std::io::Error::from)?;
    let stat = posix::fstat(&fd).map_err(std::io::Error::from)?;
    validate_stat(&stat, false, true)?;
    if stat.st_size != 0 {
        vortex_bail!("Scratch lock must be an empty private file");
    }
    Ok(File::from(fd))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootRecord {
    version: u32,
    data_path: PathBuf,
    directory: Identity,
    parent: Identity,
    lock: Identity,
    registry: Identity,
    work: Identity,
    uid: u32,
    cursor: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum State {
    Allocating,
    Live,
    Collecting,
    Finishing,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Progress {
    Present,
    Pending,
    Removed,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    path: String,
    directory: bool,
    identity: Identity,
    bytes: u64,
    progress: Progress,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    root: Identity,
    owner: String,
    state: State,
    work: Option<Identity>,
    payload: Option<Identity>,
    lock: Option<Identity>,
    plan: Vec<Entry>,
}

struct Root {
    directory: OwnedFd,
    registry: OwnedFd,
    work: OwnedFd,
    _lock: File,
}

pub fn namespace(parent: &Path, data_path: &Path) -> VortexResult<PathBuf> {
    if !parent.is_absolute()
        || !data_path.is_absolute()
        || parent.canonicalize()? != parent
        || data_path.canonicalize()? != data_path
        || parent.starts_with(data_path)
    {
        vortex_bail!("Scratch namespace requires canonical paths outside managed data");
    }
    let version = file_version(
        data_path
            .canonicalize()?
            .to_str()
            .ok_or_else(|| vortex_err!("Scratch data path must be UTF-8"))?
            .as_bytes(),
    );
    Ok(parent.join(format!(
        ".ducklake-vortex-query-v1-{}-{}",
        uid(),
        &version[7..]
    )))
}

impl Root {
    fn open(path: &Path, data_path: &Path, create: bool, wait: bool) -> VortexResult<Self> {
        let parent_path = path
            .parent()
            .ok_or_else(|| vortex_err!("Scratch namespace parent required"))?;
        if namespace(parent_path, data_path)? != path {
            vortex_bail!("Scratch namespace does not belong to this data path");
        }
        let parent =
            posix::open(parent_path, DIRECTORY, Mode::empty()).map_err(std::io::Error::from)?;
        let name = path
            .file_name()
            .and_then(|x| x.to_str())
            .ok_or_else(|| vortex_err!("Invalid scratch namespace name"))?;
        let lock_name = format!("{name}.lock");
        let lock = private_file(&parent, &lock_name, create)?;
        let operation = if wait {
            FlockOperation::LockExclusive
        } else {
            FlockOperation::NonBlockingLockExclusive
        };
        match posix::flock(&lock, operation) {
            Ok(()) => (),
            Err(rustix::io::Errno::WOULDBLOCK) => vortex_bail!("Scratch namespace is busy"),
            Err(error) => return Err(std::io::Error::from(error).into()),
        }
        let lock_identity = identity(&posix::fstat(&lock).map_err(std::io::Error::from)?);
        if exists(&parent, name)?.is_none() {
            if !create {
                vortex_bail!("Registered scratch namespace does not exist");
            }
            let staging = tempfile::Builder::new()
                .prefix(".ducklake-vortex-root-init-")
                .permissions(std::fs::Permissions::from_mode(0o700))
                .tempdir_in(parent_path)?;
            let dir = posix::open(staging.path(), DIRECTORY, Mode::empty())
                .map_err(std::io::Error::from)?;
            posix::mkdirat(&dir, "registry", PRIVATE).map_err(std::io::Error::from)?;
            posix::mkdirat(&dir, "work", PRIVATE).map_err(std::io::Error::from)?;
            let record = RootRecord {
                version: 1,
                data_path: data_path.into(),
                directory: identity(&posix::fstat(&dir).map_err(std::io::Error::from)?),
                parent: identity(&posix::fstat(&parent).map_err(std::io::Error::from)?),
                lock: lock_identity,
                registry: identity(
                    &posix::fstat(&child(&dir, "registry")?).map_err(std::io::Error::from)?,
                ),
                work: identity(&posix::fstat(&child(&dir, "work")?).map_err(std::io::Error::from)?),
                uid: uid(),
                cursor: String::new(),
            };
            let fd = posix::openat(
                &dir,
                "root.json",
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW,
                FILE_MODE,
            )
            .map_err(std::io::Error::from)?;
            let mut file = File::from(fd);
            file.write_all(&serde_json::to_vec(&record).map_err(|e| vortex_err!("{e}"))?)?;
            file.sync_all()?;
            posix::fsync(&dir).map_err(std::io::Error::from)?;
            posix::renameat_with(
                &parent,
                staging.path().file_name().unwrap(),
                &parent,
                name,
                posix::RenameFlags::NOREPLACE,
            )
            .map_err(std::io::Error::from)?;
            let _ = staging.keep();
            posix::fsync(&parent).map_err(std::io::Error::from)?;
        }
        let directory = child(&parent, name)?;
        validate_stat(
            &posix::fstat(&directory).map_err(std::io::Error::from)?,
            true,
            true,
        )?;
        let record: RootRecord = read_json(&directory, "root.json")?;
        if record.version != 1
            || record.uid != uid()
            || record.data_path != data_path
            || record.directory
                != identity(&posix::fstat(&directory).map_err(std::io::Error::from)?)
            || record.parent != identity(&posix::fstat(&parent).map_err(std::io::Error::from)?)
            || record.lock != lock_identity
            || (!record.cursor.is_empty()
                && uuid::Uuid::parse_str(&record.cursor)
                    .map(|u| u.to_string() != record.cursor)
                    .unwrap_or(true))
        {
            vortex_bail!("Scratch namespace registration identity mismatch");
        }
        let allowed = BTreeSet::from(["root.json", "registry", "work", ".record.tmp"]);
        if names(&directory)?
            .iter()
            .any(|name| !allowed.contains(name.as_str()))
        {
            vortex_bail!("Unknown file in scratch protocol namespace");
        }
        // This exact reserved metadata slot is serialized by the root lock. It never contains query payload.
        if let Some(stat) = exists(&directory, ".record.tmp")? {
            validate_stat(&stat, false, true)?;
            if stat.st_size < 0 || stat.st_size as usize > LIMITS.max_manifest_bytes {
                vortex_bail!("Invalid scratch metadata staging size");
            }
            posix::unlinkat(&directory, ".record.tmp", AtFlags::empty())
                .map_err(std::io::Error::from)?;
            posix::fsync(&directory).map_err(std::io::Error::from)?;
        }
        let registry = child(&directory, "registry")?;
        let work = child(&directory, "work")?;
        validate_stat(
            &posix::fstat(&registry).map_err(std::io::Error::from)?,
            true,
            true,
        )?;
        validate_stat(
            &posix::fstat(&work).map_err(std::io::Error::from)?,
            true,
            true,
        )?;
        same(&registry, record.registry, true, true)?;
        same(&work, record.work, true, true)?;
        Ok(Self {
            directory,
            registry,
            work,
            _lock: lock,
        })
    }
    fn write<T: Serialize>(&self, directory: &OwnedFd, name: &str, record: &T) -> VortexResult<()> {
        let bytes = serde_json::to_vec(record).map_err(|e| vortex_err!("{e}"))?;
        if bytes.len() > LIMITS.max_manifest_bytes {
            vortex_bail!("Scratch deletion manifest exceeds byte budget");
        }
        let fd = posix::openat(
            &self.directory,
            ".record.tmp",
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW,
            FILE_MODE,
        )
        .map_err(std::io::Error::from)?;
        let mut file = File::from(fd);
        file.write_all(&bytes)?;
        file.sync_all()?;
        posix::renameat(&self.directory, ".record.tmp", directory, name)
            .map_err(std::io::Error::from)?;
        posix::fsync(directory).map_err(std::io::Error::from)?;
        posix::fsync(&self.directory).map_err(std::io::Error::from)?;
        Ok(())
    }
    fn save(&self, record: &Record) -> VortexResult<()> {
        self.write(&self.registry, &record.owner, record)
    }
    fn load(&self, name: &str) -> VortexResult<Record> {
        let parsed = uuid::Uuid::parse_str(name)
            .map_err(|_| vortex_err!("Unknown scratch registration filename"))?;
        if parsed.to_string() != name {
            vortex_bail!("Invalid scratch owner identity");
        }
        let record: Record = read_json(&self.registry, name)?;
        if record.version != 1
            || record.owner != name
            || record.root
                != identity(&posix::fstat(&self.directory).map_err(std::io::Error::from)?)
        {
            vortex_bail!("Scratch owner registration mismatch");
        }
        if (record.state != State::Allocating
            && record.state != State::Finishing
            && (record.work.is_none() || record.payload.is_none() || record.lock.is_none()))
            || (record.state == State::Live && !record.plan.is_empty())
            || record
                .plan
                .iter()
                .filter(|e| e.progress == Progress::Pending)
                .count()
                > 1
        {
            vortex_bail!("Invalid scratch ownership state");
        }
        Ok(record)
    }
}

fn validate_work(work: &OwnedFd, record: &Record, finishing: bool) -> VortexResult<()> {
    validate_stat(
        &posix::fstat(work).map_err(std::io::Error::from)?,
        true,
        true,
    )?;
    if let Some(expected) = record.work {
        same(work, expected, true, true)?;
    }
    if names(work)?
        .iter()
        .any(|name| name != "owner.lock" && name != "payload")
    {
        vortex_bail!("Unknown file outside registered scratch payload");
    }
    for (name, expected, directory) in [
        ("owner.lock", record.lock, false),
        ("payload", record.payload, true),
    ] {
        match exists(work, name)? {
            Some(stat) => {
                validate_stat(&stat, directory, true)?;
                if expected.is_some_and(|id| id != identity(&stat)) {
                    vortex_bail!("Scratch control identity changed");
                }
            }
            None if finishing || record.state == State::Allocating => (),
            None => vortex_bail!("Registered scratch control is missing"),
        }
    }
    Ok(())
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && path.split('/').count() <= 32
        && Path::new(path)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
        && !path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
}
fn parent_fd(payload: &OwnedFd, path: &str) -> VortexResult<(OwnedFd, String)> {
    if !valid_path(path) {
        vortex_bail!("Invalid relative scratch payload path");
    }
    let mut directory = child(payload, ".")?;
    let parts: Vec<_> = path.split('/').collect();
    for component in &parts[..parts.len() - 1] {
        directory = child(&directory, component)?;
    }
    Ok((directory, parts[parts.len() - 1].into()))
}

fn inventory(directory: &OwnedFd) -> VortexResult<Vec<Entry>> {
    fn visit(
        dir: &OwnedFd,
        prefix: &str,
        result: &mut Vec<Entry>,
        bytes: &mut usize,
    ) -> VortexResult<()> {
        for name in names(dir)? {
            let path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            if !valid_path(&path) {
                vortex_bail!("Scratch payload path exceeds its ownership bounds");
            }
            let stat = exists(dir, &name)?
                .ok_or_else(|| vortex_err!("Scratch payload changed during inventory"))?;
            let directory = is_dir(&stat);
            validate_stat(&stat, directory, false)?;
            if stat.st_size < 0
                || (!directory && stat.st_size as u64 > LIMITS.max_artifact_bytes as u64)
            {
                vortex_bail!("Scratch payload file exceeds artifact budget");
            }
            let entry = Entry {
                path: path.clone(),
                directory,
                identity: identity(&stat),
                bytes: if directory { 0 } else { stat.st_size as u64 },
                progress: Progress::Present,
            };
            *bytes += serde_json::to_vec(&entry)
                .map_err(|e| vortex_err!("{e}"))?
                .len()
                + 1;
            if *bytes > LIMITS.max_manifest_bytes {
                vortex_bail!("Scratch ownership inventory exceeds manifest budget");
            }
            if directory {
                visit(&child(dir, &name)?, &path, result, bytes)?;
            }
            result.push(entry);
        }
        Ok(())
    }
    let mut result = vec![];
    visit(directory, "", &mut result, &mut 0)?;
    Ok(result)
}

fn validate_plan(payload: &OwnedFd, plan: &[Entry]) -> VortexResult<()> {
    let current = inventory(payload)?;
    let mut actual: BTreeMap<_, _> = current.into_iter().map(|e| (e.path.clone(), e)).collect();
    let mut seen = BTreeSet::new();
    for entry in plan {
        if !valid_path(&entry.path) || !seen.insert(entry.path.as_str()) {
            vortex_bail!("Invalid scratch deletion manifest path");
        }
        match actual.remove(&entry.path) {
            Some(found)
                if entry.progress != Progress::Removed
                    && found.identity == entry.identity
                    && found.bytes == entry.bytes
                    && found.directory == entry.directory =>
            {
                ()
            }
            None if entry.progress != Progress::Present => (),
            _ => vortex_bail!("Scratch deletion manifest filesystem mismatch"),
        }
    }
    if !actual.is_empty() {
        vortex_bail!("Unknown payload appeared after scratch deletion plan");
    }
    Ok(())
}

fn finish(
    root: &Root,
    record: &Record,
    check: &dyn Fn(&str) -> VortexResult<()>,
) -> VortexResult<()> {
    if record.state != State::Finishing
        || record.plan.iter().any(|e| e.progress != Progress::Removed)
    {
        vortex_bail!("Scratch owner lacks a committed finishing intent");
    }
    check("scratch_before_remove_view")?;
    if let Some(stat) = exists(&root.work, &record.owner)? {
        validate_stat(&stat, true, true)?;
        let work = child(&root.work, &record.owner)?;
        validate_work(&work, record, true)?;
        if exists(&work, "payload")?.is_some() {
            let payload = child(&work, "payload")?;
            if !names(&payload)?.is_empty() {
                vortex_bail!("Finishing scratch payload is not empty");
            }
            posix::unlinkat(&work, "payload", AtFlags::REMOVEDIR).map_err(std::io::Error::from)?;
            posix::fsync(&work).map_err(std::io::Error::from)?;
        }
        if exists(&work, "owner.lock")?.is_some() {
            posix::unlinkat(&work, "owner.lock", AtFlags::empty()).map_err(std::io::Error::from)?;
            posix::fsync(&work).map_err(std::io::Error::from)?;
        }
        posix::unlinkat(&root.work, &record.owner, AtFlags::REMOVEDIR)
            .map_err(std::io::Error::from)?;
        posix::fsync(&root.work).map_err(std::io::Error::from)?;
    }
    check("scratch_after_remove_view")?;
    posix::unlinkat(&root.registry, &record.owner, AtFlags::empty())
        .map_err(std::io::Error::from)?;
    posix::fsync(&root.registry).map_err(std::io::Error::from)?;
    Ok(())
}

fn finish_allocation(root: &Root, record: &mut Record) -> VortexResult<()> {
    if let Some(_) = exists(&root.work, &record.owner)? {
        let work = child(&root.work, &record.owner)?;
        validate_work(&work, record, false)?;
        if exists(&work, "payload")?.is_some() && !names(&child(&work, "payload")?)?.is_empty() {
            vortex_bail!("Unpublished scratch allocation contains unknown payload");
        }
        record.work = Some(identity(
            &posix::fstat(&work).map_err(std::io::Error::from)?,
        ));
        if let Some(stat) = exists(&work, "payload")? {
            record.payload = Some(identity(&stat));
        }
        if let Some(stat) = exists(&work, "owner.lock")? {
            record.lock = Some(identity(&stat));
        }
    }
    record.state = State::Finishing;
    root.save(record)?;
    finish(root, record, &|_| Ok(()))
}

#[derive(Default, Serialize)]
pub struct Collection {
    pub root: PathBuf,
    pub registered_views: u64,
    pub examined_views: u64,
    pub blocked_views: u64,
    pub collected_objects: u64,
    pub collected_files: u64,
    pub collected_bytes: u64,
    pub removed_views: u64,
    pub remaining_views: u64,
}

pub fn collect(
    data_path: &Path,
    path: &Path,
    max_objects: u64,
    max_bytes: u64,
    check: &dyn Fn(&str) -> VortexResult<()>,
) -> VortexResult<Collection> {
    if !(1..=16).contains(&max_objects) || !(1..=64 * 1024 * 1024).contains(&max_bytes) {
        vortex_bail!("Invalid scratch collection budgets");
    }
    check("scratch_collect_start")?;
    let root = Root::open(path, data_path, false, false)?;
    let mut info: RootRecord = read_json(&root.directory, "root.json")?;
    if !info.cursor.is_empty()
        && uuid::Uuid::parse_str(&info.cursor)
            .map(|u| u.to_string() != info.cursor)
            .unwrap_or(true)
    {
        vortex_bail!("Invalid scratch collection cursor");
    }
    let registered = names(&root.registry)?;
    // Validate every name without adopting any unknown directory from work/ or the temporary parent.
    for name in &registered {
        if uuid::Uuid::parse_str(name)
            .map(|u| u.to_string() != *name)
            .unwrap_or(true)
        {
            vortex_bail!("Unknown scratch registration filename");
        }
    }
    let ordered: Vec<_> = registered
        .iter()
        .filter(|n| *n > &info.cursor)
        .chain(registered.iter().filter(|n| *n <= &info.cursor))
        .take(max_objects as usize)
        .cloned()
        .collect();
    let mut result = Collection {
        root: path.into(),
        registered_views: registered.len() as u64,
        ..Default::default()
    };
    for name in ordered {
        if result.collected_objects == max_objects {
            break;
        }
        check("scratch_collect_owner")?;
        let mut record = root.load(&name)?;
        result.examined_views += 1;
        let work = if exists(&root.work, &name)?.is_some() {
            Some(child(&root.work, &name)?)
        } else {
            None
        };
        let mut held = None;
        if let Some(work) = &work {
            validate_work(work, &record, record.state == State::Finishing)?;
            if exists(work, "owner.lock")?.is_some() {
                let lock = private_file(work, "owner.lock", false)?;
                match posix::flock(&lock, FlockOperation::NonBlockingLockExclusive) {
                    Ok(()) => held = Some(lock),
                    Err(rustix::io::Errno::WOULDBLOCK) => {
                        result.blocked_views += 1;
                        info.cursor = name;
                        root.write(&root.directory, "root.json", &info)?;
                        continue;
                    }
                    Err(error) => return Err(std::io::Error::from(error).into()),
                }
            }
        } else if record.state != State::Allocating && record.state != State::Finishing {
            vortex_bail!("Registered scratch work directory is missing");
        }
        if record.state == State::Allocating {
            finish_allocation(&root, &mut record)?;
            result.collected_objects += 1;
            result.removed_views += 1;
        } else if record.state == State::Finishing {
            finish(&root, &record, check)?;
            result.collected_objects += 1;
            result.removed_views += 1;
        } else {
            let work = work
                .as_ref()
                .ok_or_else(|| vortex_err!("Scratch work directory required"))?;
            let payload = child(work, "payload")?;
            if record.state == State::Live {
                record.plan = inventory(&payload)?;
                record.state = State::Collecting;
                root.save(&record)?;
                check("scratch_plan_published")?;
            }
            validate_plan(&payload, &record.plan)?;
            let order: Vec<_> = (0..record.plan.len())
                .filter(|i| record.plan[*i].progress == Progress::Pending)
                .chain(
                    (0..record.plan.len())
                        .filter(|i| record.plan[*i].progress != Progress::Pending),
                )
                .collect();
            for i in order {
                if result.collected_objects == max_objects {
                    break;
                }
                let entry = record.plan[i].clone();
                if entry.progress == Progress::Removed {
                    continue;
                }
                let (parent, component) = parent_fd(&payload, &entry.path)?;
                if let Some(stat) = exists(&parent, &component)? {
                    validate_stat(&stat, entry.directory, false)?;
                    if identity(&stat) != entry.identity
                        || (!entry.directory && stat.st_size as u64 != entry.bytes)
                    {
                        vortex_bail!("Scratch unlink identity changed");
                    }
                    if entry.bytes > max_bytes - result.collected_bytes {
                        if entry.progress == Progress::Pending {
                            break;
                        }
                        continue;
                    }
                    if entry.directory && !names(&child(&parent, &component)?)?.is_empty() {
                        continue;
                    }
                    record.plan[i].progress = Progress::Pending;
                    root.save(&record)?;
                    check("scratch_before_unlink")?;
                    posix::unlinkat(
                        &parent,
                        &component,
                        if entry.directory {
                            AtFlags::REMOVEDIR
                        } else {
                            AtFlags::empty()
                        },
                    )
                    .map_err(std::io::Error::from)?;
                    posix::fsync(&parent).map_err(std::io::Error::from)?;
                    result.collected_objects += 1;
                    result.collected_files += u64::from(!entry.directory);
                    result.collected_bytes += entry.bytes;
                    check("scratch_after_unlink")?;
                } else if entry.progress != Progress::Pending {
                    vortex_bail!("Scratch file vanished without a deletion intent");
                }
                record.plan[i].progress = Progress::Removed;
                root.save(&record)?;
            }
            if record.plan.iter().all(|e| e.progress == Progress::Removed)
                && result.collected_objects < max_objects
            {
                record.state = State::Finishing;
                root.save(&record)?;
                finish(&root, &record, check)?;
                result.collected_objects += 1;
                result.removed_views += 1;
            }
        }
        drop(held);
        info.cursor = name;
        root.write(&root.directory, "root.json", &info)?;
    }
    result.remaining_views = names(&root.registry)?.len() as u64;
    Ok(result)
}

pub struct OwnedScratch {
    root: PathBuf,
    data_path: PathBuf,
    owner: String,
    payload: PathBuf,
    _lock: File,
}
impl OwnedScratch {
    pub fn create(
        parent: &Path,
        data_path: &Path,
        check: &dyn Fn(&str) -> VortexResult<()>,
    ) -> VortexResult<Self> {
        let path = namespace(parent, data_path)?;
        let root = Root::open(&path, data_path, true, true)?;
        let mut record = Record {
            version: 1,
            root: identity(&posix::fstat(&root.directory).map_err(std::io::Error::from)?),
            owner: uuid::Uuid::new_v4().to_string(),
            state: State::Allocating,
            work: None,
            payload: None,
            lock: None,
            plan: vec![],
        };
        if exists(&root.work, &record.owner)?.is_some() {
            vortex_bail!("Unregistered scratch allocation path already exists");
        }
        root.save(&record)?;
        let initialized = (|| {
            check("scratch_allocated")?;
            posix::mkdirat(&root.work, &record.owner, PRIVATE).map_err(std::io::Error::from)?;
            let work = child(&root.work, &record.owner)?;
            posix::mkdirat(&work, "payload", PRIVATE).map_err(std::io::Error::from)?;
            let payload = child(&work, "payload")?;
            let lock = private_file(&work, "owner.lock", true)?;
            posix::flock(&lock, FlockOperation::LockExclusive).map_err(std::io::Error::from)?;
            record.work = Some(identity(
                &posix::fstat(&work).map_err(std::io::Error::from)?,
            ));
            record.payload = Some(identity(
                &posix::fstat(&payload).map_err(std::io::Error::from)?,
            ));
            record.lock = Some(identity(
                &posix::fstat(&lock).map_err(std::io::Error::from)?,
            ));
            record.state = State::Live;
            posix::fsync(&payload).map_err(std::io::Error::from)?;
            posix::fsync(&work).map_err(std::io::Error::from)?;
            posix::fsync(&root.work).map_err(std::io::Error::from)?;
            root.save(&record)?;
            Ok(lock)
        })();
        let lock = match initialized {
            Ok(lock) => lock,
            Err(error) => {
                // No provider can write payload before registration completes.
                finish_allocation(&root, &mut record)?;
                return Err(error);
            }
        };
        let payload = path.join("work").join(&record.owner).join("payload");
        Ok(Self {
            root: path,
            data_path: data_path.into(),
            owner: record.owner,
            payload,
            _lock: lock,
        })
    }
    pub fn path(&self) -> &Path {
        &self.payload
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn close(self) -> VortexResult<()> {
        let root = Root::open(&self.root, &self.data_path, false, true)?;
        let mut record = root.load(&self.owner)?;
        if record.state != State::Live {
            vortex_bail!("Live scratch owner changed state");
        }
        let work = child(&root.work, &self.owner)?;
        validate_work(&work, &record, false)?;
        let payload = child(&work, "payload")?;
        if !names(&payload)?.is_empty() {
            vortex_bail!(
                "Closing scratch still owns unexpected payload; explicit collection required"
            );
        }
        record.state = State::Finishing;
        root.save(&record)?;
        finish(&root, &record, &|_| Ok(()))
    }
}
