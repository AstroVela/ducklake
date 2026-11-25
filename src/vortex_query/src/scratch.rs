//! Registered native-query scratch; only explicit, authenticated collection reclaims crashed owners.
use crate::LIMITS;
#[cfg(not(test))]
use rustix::fs::fsync as sync_removal_parent;
use rustix::fs::{self as posix, AtFlags, FlockOperation, Mode, OFlags, Stat};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::thread;
use std::time::Duration;
#[cfg(test)]
use tests::sync_removal_parent;
use vortex::error::{VortexResult, vortex_bail, vortex_err};
use vortex_index::file::file_version;

const PRIVATE: Mode = Mode::from_bits_truncate(0o700);
const FILE_MODE: Mode = Mode::from_bits_truncate(0o600);
const DIRECTORY: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const FILE_READ: OFlags = OFlags::RDONLY
    .union(OFlags::NONBLOCK)
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
    validate_identity(&stat, expected, directory, private)
}
fn validate_identity(
    stat: &Stat,
    expected: Identity,
    directory: bool,
    private: bool,
) -> VortexResult<()> {
    validate_stat(stat, directory, private)?;
    if identity(stat) != expected {
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
fn open_record(dir: &OwnedFd, name: &str) -> VortexResult<OwnedFd> {
    let fd = posix::openat(dir, name, FILE_READ, Mode::empty()).map_err(std::io::Error::from)?;
    let stat = posix::fstat(&fd).map_err(std::io::Error::from)?;
    validate_stat(&stat, false, true)?;
    if stat.st_size < 0 || stat.st_size as usize > LIMITS.max_manifest_bytes {
        vortex_bail!("Scratch record exceeds manifest budget");
    }
    Ok(fd)
}
fn decode_record<T: serde::de::DeserializeOwned>(fd: OwnedFd) -> VortexResult<T> {
    let mut bytes = vec![];
    File::from(fd)
        .take(LIMITS.max_manifest_bytes as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > LIMITS.max_manifest_bytes {
        vortex_bail!("Scratch record exceeds manifest budget");
    }
    serde_json::from_slice(&bytes).map_err(|e| vortex_err!("Invalid scratch record: {e}"))
}
fn read_json<T: serde::de::DeserializeOwned>(dir: &OwnedFd, name: &str) -> VortexResult<T> {
    decode_record(open_record(dir, name)?)
}
fn private_file(dir: &OwnedFd, name: &str, create: bool) -> VortexResult<File> {
    let flags = OFlags::RDWR
        | OFlags::NONBLOCK
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
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    path: String,
    directory: bool,
    identity: Identity,
    bytes: u64,
    progress: Progress,
}
#[derive(PartialEq, Eq, Serialize, Deserialize)]
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
    parent_path: PathBuf,
    name: String,
    registration: RootRecord,
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
    fn open(
        path: &Path,
        data_path: &Path,
        create: bool,
        wait_check: Option<&dyn Fn(&str) -> VortexResult<()>>,
    ) -> VortexResult<Self> {
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
        loop {
            match posix::flock(&lock, FlockOperation::NonBlockingLockExclusive) {
                Ok(()) => break,
                Err(rustix::io::Errno::WOULDBLOCK) => {
                    let Some(check) = wait_check else {
                        vortex_bail!("Scratch namespace is busy");
                    };
                    check("scratch_wait_root_lock")?;
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(std::io::Error::from(error).into()),
            }
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
        let registry = child(&directory, "registry")?;
        let work = child(&directory, "work")?;
        same(&registry, record.registry, true, true)?;
        same(&work, record.work, true, true)?;
        let root = Self {
            parent_path: parent_path.to_owned(),
            name: name.to_owned(),
            registration: record,
            directory,
            registry,
            work,
            _lock: lock,
        };
        root.validate_bindings()?;
        // Only an authenticated root may discard its reserved metadata staging slot.
        if let Some(stat) = exists(&root.directory, ".record.tmp")? {
            validate_stat(&stat, false, true)?;
            if stat.st_size < 0 || stat.st_size as usize > LIMITS.max_manifest_bytes {
                vortex_bail!("Invalid scratch metadata staging size");
            }
            root.unlink(&root.directory, ".record.tmp", AtFlags::empty())?;
            posix::fsync(&root.directory).map_err(std::io::Error::from)?;
        }
        Ok(root)
    }
    fn validate_bindings(&self) -> VortexResult<()> {
        let registered = &self.registration;
        let parent = posix::open(&self.parent_path, DIRECTORY, Mode::empty())
            .map_err(std::io::Error::from)?;
        if self.parent_path.canonicalize()? != self.parent_path
            || identity(&posix::fstat(&parent).map_err(std::io::Error::from)?) != registered.parent
        {
            vortex_bail!("Scratch namespace parent changed");
        }
        // Open FDs pin objects. Their names must still designate those objects at the authorized path.
        let check_name = |parent: &OwnedFd, name: &str, expected: Identity, directory| {
            let stat = posix::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW)
                .map_err(std::io::Error::from)?;
            validate_identity(&stat, expected, directory, true)
        };
        check_name(&parent, &self.name, registered.directory, true)?;
        check_name(
            &parent,
            &format!("{}.lock", self.name),
            registered.lock,
            false,
        )?;
        check_name(&self.directory, "registry", registered.registry, true)?;
        check_name(&self.directory, "work", registered.work, true)
    }
    fn checkpoint(
        &self,
        stage: &str,
        check: &dyn Fn(&str) -> VortexResult<()>,
    ) -> VortexResult<()> {
        check(stage)?;
        self.validate_bindings()
    }
    fn unlink(&self, parent: &OwnedFd, name: &str, flags: AtFlags) -> VortexResult<()> {
        self.validate_bindings()?;
        posix::unlinkat(parent, name, flags).map_err(std::io::Error::from)?;
        Ok(())
    }
    fn write<T: Serialize>(&self, directory: &OwnedFd, name: &str, record: &T) -> VortexResult<()> {
        let bytes = serde_json::to_vec(record).map_err(|e| vortex_err!("{e}"))?;
        if bytes.len() > LIMITS.max_manifest_bytes {
            vortex_bail!("Scratch deletion manifest exceeds byte budget");
        }
        self.validate_bindings()?;
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
        self.validate_bindings()?;
        posix::renameat(&self.directory, ".record.tmp", directory, name)
            .map_err(std::io::Error::from)?;
        posix::fsync(directory).map_err(std::io::Error::from)?;
        posix::fsync(&self.directory).map_err(std::io::Error::from)?;
        Ok(())
    }
    fn save(&self, record: &Record) -> VortexResult<()> {
        self.write(&self.registry, &record.owner, record)
    }
    fn advance_cursor(&mut self, name: String) -> VortexResult<()> {
        self.registration.cursor = name;
        self.write(&self.directory, "root.json", &self.registration)
    }
    fn registration_checkpoint(
        &self,
        record: &Record,
        stage: &str,
        check: &dyn Fn(&str) -> VortexResult<()>,
    ) -> VortexResult<()> {
        let original = open_record(&self.registry, &record.owner)?;
        self.checkpoint(stage, check)?;
        let current = open_record(&self.registry, &record.owner)?;
        verify_registration(&original, current, record)
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

fn verify_registration(original: &OwnedFd, current: OwnedFd, record: &Record) -> VortexResult<()> {
    let original_stat = posix::fstat(original).map_err(std::io::Error::from)?;
    let current_stat = posix::fstat(&current).map_err(std::io::Error::from)?;
    if identity(&original_stat) != identity(&current_stat)
        || decode_record::<Record>(current)? != *record
    {
        vortex_bail!("Scratch registration identity or contents changed");
    }
    Ok(())
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

fn validate_unlink_entry(stat: &Stat, entry: &Entry) -> VortexResult<()> {
    validate_stat(stat, entry.directory, false)?;
    if identity(stat) != entry.identity || (!entry.directory && stat.st_size as u64 != entry.bytes)
    {
        vortex_bail!("Scratch unlink identity changed");
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
    root.registration_checkpoint(record, "scratch_before_remove_view", check)?;
    if let Some(stat) = exists(&root.work, &record.owner)? {
        validate_stat(&stat, true, true)?;
        let work = child(&root.work, &record.owner)?;
        validate_work(&work, record, true)?;
        if exists(&work, "payload")?.is_some() {
            let payload = child(&work, "payload")?;
            if !names(&payload)?.is_empty() {
                vortex_bail!("Finishing scratch payload is not empty");
            }
            root.unlink(&work, "payload", AtFlags::REMOVEDIR)?;
            posix::fsync(&work).map_err(std::io::Error::from)?;
        }
        if exists(&work, "owner.lock")?.is_some() {
            root.unlink(&work, "owner.lock", AtFlags::empty())?;
            posix::fsync(&work).map_err(std::io::Error::from)?;
        }
        root.unlink(&root.work, &record.owner, AtFlags::REMOVEDIR)?;
    }
    // A previous attempt may have removed the owner before its parent was synced.
    sync_removal_parent(&root.work).map_err(std::io::Error::from)?;
    root.registration_checkpoint(record, "scratch_after_remove_view", check)?;
    root.unlink(&root.registry, &record.owner, AtFlags::empty())?;
    posix::fsync(&root.registry).map_err(std::io::Error::from)?;
    Ok(())
}

fn finish_allocation(
    root: &Root,
    record: &mut Record,
    check: &dyn Fn(&str) -> VortexResult<()>,
) -> VortexResult<()> {
    root.validate_bindings()?;
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
    finish(root, record, check)
}

fn registered_window(
    registry: &OwnedFd,
    cursor: &str,
    max_objects: usize,
    check: &dyn Fn(&str) -> VortexResult<()>,
) -> VortexResult<(u64, Vec<String>)> {
    let mut first = BTreeSet::<String>::new();
    let mut wrapped = BTreeSet::<String>::new();
    let mut count = 0;
    for entry in posix::Dir::read_from(registry).map_err(std::io::Error::from)? {
        let entry = entry.map_err(std::io::Error::from)?;
        let name = entry.file_name().to_str().map_err(|e| vortex_err!("{e}"))?;
        if name == "." || name == ".." {
            continue;
        }
        if uuid::Uuid::parse_str(name)
            .map(|id| id.to_string() != name)
            .unwrap_or(true)
        {
            vortex_bail!("Unknown scratch registration filename");
        }
        count += 1;
        if count % 256 == 0 {
            check("scratch_scan_registry")?;
        }
        let page = if name > cursor {
            &mut first
        } else {
            &mut wrapped
        };
        if page.len() < max_objects || page.last().is_some_and(|last| name < last.as_str()) {
            page.insert(name.to_owned());
            if page.len() > max_objects {
                page.pop_last();
            }
        }
    }
    Ok((
        count,
        first.into_iter().chain(wrapped).take(max_objects).collect(),
    ))
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
    let mut root = Root::open(path, data_path, false, None)?;
    let (registered_views, ordered) = registered_window(
        &root.registry,
        &root.registration.cursor,
        max_objects as usize,
        &|stage| root.checkpoint(stage, check),
    )?;
    let mut result = Collection {
        root: path.into(),
        registered_views,
        ..Default::default()
    };
    for name in ordered {
        if result.collected_objects == max_objects {
            break;
        }
        root.checkpoint("scratch_collect_owner", check)?;
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
                        root.advance_cursor(name)?;
                        continue;
                    }
                    Err(error) => return Err(std::io::Error::from(error).into()),
                }
            }
        } else if record.state != State::Allocating && record.state != State::Finishing {
            vortex_bail!("Registered scratch work directory is missing");
        }
        if record.state == State::Allocating {
            finish_allocation(&root, &mut record, check)?;
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
                root.registration_checkpoint(&record, "scratch_plan_published", check)?;
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
                    validate_unlink_entry(&stat, &entry)?;
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
                    root.registration_checkpoint(&record, "scratch_before_unlink", check)?;
                    // Reopen the registered path after the checkpoint before touching payload.
                    let work = child(&root.work, &record.owner)?;
                    validate_work(&work, &record, false)?;
                    let current_payload = child(&work, "payload")?;
                    same(
                        &current_payload,
                        identity(&posix::fstat(&payload).map_err(std::io::Error::from)?),
                        true,
                        true,
                    )?;
                    let (current_parent, component) = parent_fd(&current_payload, &entry.path)?;
                    same(
                        &current_parent,
                        identity(&posix::fstat(&parent).map_err(std::io::Error::from)?),
                        true,
                        false,
                    )?;
                    let stat = exists(&current_parent, &component)?.ok_or_else(|| {
                        vortex_err!("Scratch unlink entry disappeared after its checkpoint")
                    })?;
                    validate_unlink_entry(&stat, &entry)?;
                    root.unlink(
                        &current_parent,
                        &component,
                        if entry.directory {
                            AtFlags::REMOVEDIR
                        } else {
                            AtFlags::empty()
                        },
                    )?;
                    sync_removal_parent(&current_parent).map_err(std::io::Error::from)?;
                    result.collected_objects += 1;
                    result.collected_files += u64::from(!entry.directory);
                    result.collected_bytes += entry.bytes;
                    root.registration_checkpoint(&record, "scratch_after_unlink", check)?;
                } else if entry.progress == Progress::Pending {
                    // Make the prior unlink durable before acknowledging it in the manifest.
                    sync_removal_parent(&parent).map_err(std::io::Error::from)?;
                } else {
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
        root.advance_cursor(name)?;
    }
    root.validate_bindings()?;
    result.remaining_views = registered_views - result.removed_views;
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
        let root = Root::open(&path, data_path, true, Some(check))?;
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
            root.checkpoint("scratch_allocated", check)?;
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
                finish_allocation(&root, &mut record, &|_| Ok(()))?;
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
    pub fn close(self, check: &dyn Fn(&str) -> VortexResult<()>) -> VortexResult<()> {
        // If the wait is interrupted, dropping the owner releases its lock and leaves a registered orphan.
        let root = Root::open(&self.root, &self.data_path, false, Some(check))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    thread_local! {
        static FAIL_PARENT_SYNC: Cell<Option<Identity>> = const { Cell::new(None) };
    }

    pub(super) fn sync_removal_parent(parent: &OwnedFd) -> rustix::io::Result<()> {
        if FAIL_PARENT_SYNC.get() == Some(identity(&posix::fstat(parent)?)) {
            return Err(rustix::io::Errno::IO);
        }
        posix::fsync(parent)
    }

    #[test]
    fn recovery_syncs_missing_entries_before_acknowledging_them() -> VortexResult<()> {
        let temp = tempfile::tempdir()?;
        let parent = temp.path().join("scratch");
        let data = temp.path().join("data");
        std::fs::create_dir(&parent)?;
        std::fs::create_dir(&data)?;
        let view = OwnedScratch::create(&parent, &data, &|_| Ok(()))?;
        let root = view.root().to_owned();
        let owner = view.owner.clone();
        let payload = view.path().to_owned();
        std::fs::write(payload.join("first"), b"1234567")?;
        std::fs::write(payload.join("second"), b"1234")?;
        drop(view);

        let collect_one = |bytes| collect(&data, &root, 1, bytes, &|_| Ok(()));
        let record = || Root::open(&root, &data, false, None)?.load(&owner);
        let fail_sync = |directory: &Path, bytes| -> VortexResult<()> {
            let stat = posix::stat(directory).map_err(std::io::Error::from)?;
            FAIL_PARENT_SYNC.set(Some(identity(&stat)));
            let result = collect_one(bytes);
            FAIL_PARENT_SYNC.set(None);
            let error = result
                .err()
                .expect("collector acknowledged removal without syncing its parent");
            assert!(
                error
                    .to_string()
                    .contains(&std::io::Error::from(rustix::io::Errno::IO).to_string())
            );
            Ok(())
        };

        // First fail after unlink, then fail again on recovery of the already absent file.
        for bytes in [7, 1] {
            fail_sync(&payload, bytes)?;
            assert!(!payload.join("first").exists());
            assert_eq!(std::fs::read(payload.join("second"))?, b"1234");
            let saved = record()?;
            assert!(saved.state == State::Collecting);
            assert_eq!(saved.plan.len(), 2);
            assert!(saved.plan[0].path == "first" && saved.plan[0].progress == Progress::Pending);
            assert!(saved.plan[1].path == "second" && saved.plan[1].progress == Progress::Present);
        }
        let pass = collect_one(4)?;
        assert_eq!(
            (
                pass.collected_objects,
                pass.collected_files,
                pass.collected_bytes
            ),
            (1, 1, 4)
        );
        assert_eq!((pass.removed_views, pass.remaining_views), (0, 1));
        assert!(
            record()?
                .plan
                .iter()
                .all(|e| e.progress == Progress::Removed)
        );

        // Repeat at owner-directory removal, before the registration can be retired.
        let work = root.join("work");
        for _ in 0..2 {
            fail_sync(&work, 1)?;
            assert!(!work.join(&owner).exists());
            assert!(record()?.state == State::Finishing);
        }
        let pass = collect_one(1)?;
        assert_eq!(
            (
                pass.collected_objects,
                pass.collected_files,
                pass.collected_bytes
            ),
            (1, 0, 0)
        );
        assert_eq!((pass.removed_views, pass.remaining_views), (1, 0));
        assert_eq!(collect_one(1)?.collected_objects, 0);
        Ok(())
    }

    fn registration() -> Record {
        Record {
            version: 1,
            root: Identity {
                device: 1,
                inode: 1,
            },
            owner: uuid::Uuid::new_v4().to_string(),
            state: State::Finishing,
            work: None,
            payload: None,
            lock: None,
            plan: Vec::new(),
        }
    }

    #[test]
    fn checkpoints_and_mutations_require_both_registered_directories() -> VortexResult<()> {
        let temp = tempfile::tempdir()?;
        let parent = temp.path().join("scratch");
        let data = temp.path().join("data");
        std::fs::create_dir(&parent)?;
        std::fs::create_dir(&data)?;
        let path = namespace(&parent, &data)?;
        let mut root = Root::open(&path, &data, true, None)?;
        let record = Record {
            root: root.registration.directory,
            ..registration()
        };
        root.save(&record)?;
        root.registration_checkpoint(&record, "verify", &|_| Ok(()))?;
        let header = std::fs::read(path.join("root.json"))?;
        let registered_owner = path.join("registry").join(&record.owner);
        let contents = std::fs::read(&registered_owner)?;
        let registry = root.registration.registry;
        let work = root.registration.work;
        assert_ne!(registry, work);
        // Vary the expected identities in memory while the fixture and its permissions stay fixed.
        for (expected_registry, expected_work) in [(work, work), (registry, registry)] {
            root.registration.registry = expected_registry;
            root.registration.work = expected_work;
            let checked = std::cell::Cell::new(false);
            let checkpoint = root.registration_checkpoint(&record, "verify", &|_| {
                checked.set(true);
                Ok(())
            });
            assert!(checked.get() && checkpoint.is_err());
            assert!(root.advance_cursor(record.owner.clone()).is_err());
            assert!(
                root.unlink(&root.registry, &record.owner, AtFlags::empty())
                    .is_err()
            );
            assert_eq!(std::fs::read(path.join("root.json"))?, header);
            assert_eq!(std::fs::read(&registered_owner)?, contents);
            assert!(!path.join(".record.tmp").exists());
        }
        Ok(())
    }

    #[test]
    fn registration_checks_both_open_file_identity_and_saved_contents() -> VortexResult<()> {
        let dir = tempfile::tempdir()?;
        let directory =
            posix::open(dir.path(), DIRECTORY, Mode::empty()).map_err(std::io::Error::from)?;
        let record = registration();
        for name in ["first", "second"] {
            let mut file = private_file(&directory, name, true)?;
            serde_json::to_writer(&mut file, &record).unwrap();
        }
        let original = open_record(&directory, "first")?;
        verify_registration(&original, open_record(&directory, "first")?, &record)?;
        assert!(
            verify_registration(&original, open_record(&directory, "second")?, &record).is_err()
        );
        let changed = Record {
            state: State::Live,
            ..record
        };
        assert!(
            verify_registration(&original, open_record(&directory, "first")?, &changed).is_err()
        );
        Ok(())
    }

    #[test]
    fn control_record_opens_nonblocking_and_requires_a_regular_private_file() -> VortexResult<()> {
        let dir = tempfile::tempdir()?;
        let directory =
            posix::open(dir.path(), DIRECTORY, Mode::empty()).map_err(std::io::Error::from)?;
        private_file(&directory, "control", true)?;
        let fd = open_record(&directory, "control")?;
        let flags = posix::fcntl_getfl(&fd).map_err(std::io::Error::from)?;
        assert!(flags.contains(OFlags::NONBLOCK));
        let mut metadata = posix::fstat(&fd).map_err(std::io::Error::from)?;
        for file_type in [0o010000, 0o040000, 0o120000] {
            metadata.st_mode = file_type | 0o600;
            assert!(validate_stat(&metadata, false, true).is_err());
        }
        Ok(())
    }

    #[test]
    fn planned_file_must_still_match_before_removal() -> VortexResult<()> {
        let file = tempfile::NamedTempFile::new()?;
        let actual = posix::fstat(file.as_file()).map_err(std::io::Error::from)?;
        let mut plan = Entry {
            path: "owned".into(),
            directory: false,
            identity: identity(&actual),
            bytes: 0,
            progress: Progress::Pending,
        };
        validate_unlink_entry(&actual, &plan)?;
        plan.identity.inode ^= 1;
        assert!(validate_unlink_entry(&actual, &plan).is_err());
        plan.identity = identity(&actual);
        plan.bytes = 1;
        assert!(validate_unlink_entry(&actual, &plan).is_err());
        Ok(())
    }
}
