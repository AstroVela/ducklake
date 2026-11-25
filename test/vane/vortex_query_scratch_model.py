"""Independent filesystem assertions for the single registered query-scratch protocol."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat


def identity(path):
    value = path.lstat()
    return {"device": value.st_dev, "inode": value.st_ino}


def root_for(parent, data):
    digest = hashlib.sha256(str(data.resolve()).encode()).hexdigest()
    return parent / f".ducklake-vortex-query-v1-{os.geteuid()}-{digest}"


def registrations(root):
    return {p.name: json.loads(p.read_text()) for p in sorted((root / "registry").iterdir())}


def assert_root(root, data):
    assert root == root_for(root.parent, data)
    record = json.loads((root / "root.json").read_text())
    assert record["version"] == 1 and record["uid"] == os.geteuid()
    assert Path(record["data_path"]) == data.resolve()
    assert record["directory"] == identity(root)
    assert record["parent"] == identity(root.parent)
    assert record["lock"] == identity(root.with_name(root.name + ".lock"))
    for name in ("registry", "work"):
        assert record[name] == identity(root / name)
    for path in (root, root / "registry", root / "work"):
        value = path.lstat()
        assert stat.S_ISDIR(value.st_mode) and value.st_uid == os.geteuid() and value.st_mode & 0o077 == 0
    assert set(p.name for p in root.iterdir()) == {"root.json", "registry", "work"}


def assert_released(parent):
    """Allow authenticated empty protocol metadata, never an unrecognized leftover."""
    names = set(p.name for p in parent.iterdir())
    for path in parent.iterdir():
        if path.name.endswith(".lock"):
            assert path.name[:-5] in names and path.stat().st_size == 0
            continue
        assert re.fullmatch(r"\.ducklake-vortex-query-v1-\d+-[0-9a-f]{64}", path.name)
        data = Path(json.loads((path / "root.json").read_text())["data_path"])
        assert_root(path, data)
        assert not registrations(path) and not list((path / "work").iterdir())


def fingerprint(root):
    result = {}
    for path in sorted(root.rglob("*")):
        value = path.lstat()
        item = [
            value.st_mode,
            value.st_dev,
            value.st_ino,
            value.st_nlink,
            value.st_size,
        ]
        if stat.S_ISLNK(value.st_mode):
            item.append(os.readlink(path))
        elif stat.S_ISREG(value.st_mode):
            with path.open("rb") as stream:
                item.append(hashlib.file_digest(stream, "sha256").hexdigest())
        result[str(path.relative_to(root))] = item
    return result


def assert_budget(result, objects, bytes_):
    assert 0 <= result["collected_objects"] <= objects
    assert 0 <= result["collected_files"] <= result["collected_objects"]
    assert 0 <= result["collected_bytes"] <= bytes_
    assert 0 <= result["blocked_views"] <= result["examined_views"] <= objects
    assert 0 <= result["remaining_views"] <= result["registered_views"]
