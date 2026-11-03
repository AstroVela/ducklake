"""Sample owned file extents without treating directory growth as physical writes."""

from __future__ import annotations

import math
import os
from pathlib import Path
import stat

AREAS = ("source", "index", "metadata", "wal", "caller_references", "query_scratch", "other_scratch")


class FileInventory:
    def __init__(self, root):
        self.root = Path(root)
        self.previous = None
        self.created = dict.fromkeys(AREAS, 0)
        self.grown = dict.fromkeys(AREAS, 0)
        self.removed = dict.fromkeys(AREAS, 0)

    def sample(self):
        areas = {
            area: dict(paths=0, logical_bytes=0, attributed_inode_bytes=0, attributed_allocated_bytes=0)
            for area in AREAS
        }
        inodes, races, symlinks = {}, 0, 0

        def record(path, area, optional=False):
            nonlocal races, symlinks
            try:
                info = path.lstat()
            except FileNotFoundError:
                if not optional:
                    races += 1
                return
            if stat.S_ISLNK(info.st_mode):
                symlinks += 1
                return
            if not stat.S_ISREG(info.st_mode):
                return
            areas[area]["paths"] += 1
            areas[area]["logical_bytes"] += info.st_size
            identity = (info.st_dev, info.st_ino)
            prior = inodes.get(identity)
            if prior is None:
                inodes[identity] = (area, info.st_size, info.st_blocks * 512)
            else:
                # Traversal order assigns shared inodes to the source/index before scratch aliases.
                inodes[identity] = (prior[0], max(prior[1], info.st_size), max(prior[2], info.st_blocks * 512))

        def walk(directory, choose):
            nonlocal races, symlinks
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        try:
                            if entry.is_symlink():
                                symlinks += 1
                            elif entry.is_dir(follow_symlinks=False):
                                walk(Path(entry.path), choose)
                            else:
                                path = Path(entry.path)
                                record(path, choose(path))
                        except FileNotFoundError:
                            races += 1
            except FileNotFoundError:
                races += 1

        walk(self.root / "data", lambda path: "source")
        walk(
            self.root / "indexes",
            lambda path: (
                "caller_references" if path.parent == self.root / "indexes" and path.suffix == ".json" else "index"
            ),
        )
        record(self.root / "metadata.ducklake", "metadata")
        record(self.root / "metadata.ducklake.wal", "wal", optional=True)
        walk(
            self.root / "private",
            lambda path: (
                "query_scratch"
                if path.relative_to(self.root / "private").parts[0].startswith(".ducklake-vortex-query-")
                else "other_scratch"
            ),
        )
        for identity, (area, size, allocated) in inodes.items():
            areas[area]["attributed_inode_bytes"] += size
            areas[area]["attributed_allocated_bytes"] += allocated
            if self.previous is not None:
                if identity not in self.previous:
                    self.created[area] += size
                else:
                    self.grown[area] += max(0, size - self.previous[identity][1])
        if self.previous is not None:
            for identity, (area, size, _) in self.previous.items():
                if identity not in inodes:
                    self.removed[area] += size
        for area in AREAS:
            areas[area].update(
                observed_new_inode_bytes=self.created[area],
                observed_extent_growth_bytes=self.grown[area],
                observed_removed_inode_bytes=self.removed[area],
            )
        baseline = self.previous is None
        self.previous = inodes
        return dict(areas=areas, baseline=baseline, disappeared_during_sample=races, ignored_symlinks=symlinks)


def trend(points):
    """Retain sampled trends; a finite trace does not prove bounded steady-state storage."""
    if not points:
        return None
    first_time = points[0][0]
    values = [value for _, value in points]
    elapsed = [stamp - first_time for stamp, _ in points]
    mean_time, mean_value = sum(elapsed) / len(elapsed), sum(values) / len(values)
    denominator = sum((stamp - mean_time) ** 2 for stamp in elapsed)
    slope = (
        sum((stamp - mean_time) * (value - mean_value) for stamp, value in zip(elapsed, values)) / denominator
        if denominator
        else 0.0
    )
    windows = {}
    for stamp, value in zip(elapsed, values):
        bucket = math.floor(stamp / 60)
        windows.setdefault(bucket, []).append(value)
    return dict(
        first_monotonic_seconds=first_time,
        first=values[0],
        last=values[-1],
        minimum=min(values),
        maximum=max(values),
        slope_per_minute=slope * 60,
        seconds=elapsed[-1],
        minute_windows=[
            dict(minute=bucket, first=sample[0], last=sample[-1], minimum=min(sample), maximum=max(sample))
            for bucket, sample in sorted(windows.items())
        ],
    )


def summarize(events):
    costs = [event for event in events if event["kind"] == "cost_sample"]
    if not costs:
        return None
    # Keep drain separately: it must not conceal growth during continued ingestion.
    steady = [event for event in events if event["kind"] == "sample" and event["phase"] == "steady"]
    area_costs = [event for event in costs if event["phase"] == "steady"]
    timelines = {
        "inline_rows": [(s["usage"]["time_ns"] / 1e9, s["optimizer"]["inline_rows"]) for s in steady],
        "unindexed_rows": [(s["usage"]["time_ns"] / 1e9, s["optimizer"]["unindexed_rows"]) for s in steady],
        "eligible_snapshots": [(s["usage"]["time_ns"] / 1e9, s["source"]["eligible_snapshots"]) for s in steady],
        "scheduled_files": [(s["usage"]["time_ns"] / 1e9, s["source"]["scheduled_files"]) for s in steady],
        "pending_deletions": [(s["usage"]["time_ns"] / 1e9, s["source"]["pending_deletions"]) for s in steady],
        "remaining_builds": [(s["usage"]["time_ns"] / 1e9, s["maintenance"]["remaining_builds"]) for s in steady],
        "remaining_generations": [
            (s["usage"]["time_ns"] / 1e9, s["maintenance"]["remaining_generations"]) for s in steady
        ],
        "rss_kib": [(s["usage"]["time_ns"] / 1e9, s["usage"]["VmRSS"]) for s in steady],
    }
    for name in ["request_receipts", "receipt_logical_payload_bytes", "native_snapshots", "active_generations"]:
        timelines[name] = [(s["usage"]["time_ns"] / 1e9, s["catalog"][name]) for s in area_costs]
    areas = {}
    for area in AREAS:
        samples = [(s["usage"]["time_ns"] / 1e9, s["inventory"]["areas"][area]) for s in area_costs]
        areas[area] = dict(
            final_including_drain=costs[-1]["inventory"]["areas"][area],
            ingestion_bytes=trend([(stamp, data["attributed_inode_bytes"]) for stamp, data in samples]),
            peak_sampled_logical_bytes=max(s["inventory"]["areas"][area]["logical_bytes"] for s in costs),
        )
    return dict(
        sample_count=len(costs),
        sample_seconds=sum(s["sample_seconds"] for s in costs),
        disappeared_during_samples=sum(s["inventory"]["disappeared_during_sample"] for s in costs),
        areas=areas,
        ingestion_trends={name: trend(points) for name, points in timelines.items()},
        scope=(
            "Sampled file extents and observed inode churn, not per-layer I/O or compaction write amplification. "
            "Short-lived files between samples and inode reuse can be missed. Shared inodes count once, "
            "with source/index preceding scratch. Receipt logical payload is included in metadata/WAL, "
            "not an additional physical byte total. Process I/O is reported independently."
        ),
    )
