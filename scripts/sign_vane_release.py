#!/usr/bin/env python3
"""Isolated, stdlib-only signer for the fixed SQLite and DuckLake native data artifacts."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import stat
import subprocess
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXTENSION_NAMES = ("sqlite_scanner", "ducklake")
MAX_ARTIFACT_BYTES = 384 * 1024 * 1024
FINGERPRINTS = {
    "production": "8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb",
    "testpypi": "53779fb8f9c97e9dec9c66ff838839eb234d1a64d4b105671304820e627b5e32",
}
SYSTEM_ENV = {"PATH": "/usr/bin:/bin"}


def source_pin(manifest: str) -> str:
    if manifest not in {"vane-extension.toml", "vane-extension-release.toml"}:
        raise ValueError("signing requires a committed release-channel manifest")
    contents = subprocess.check_output(["git", "show", f"HEAD:{manifest}"], cwd=ROOT, env=SYSTEM_ENV)
    document = tomllib.loads(contents.decode("utf-8"))
    vane = document.get("vane", {})
    revision = vane.get("revision", "")
    if (
        document.get("schema_version") != 2
        or document.get("name") != "ducklake"
        or vane.get("repository") != "AstroVela/vane"
        or not isinstance(revision, str)
        or not re.fullmatch("[0-9a-f]{40}", revision)
    ):
        raise ValueError("signing requires an exact official Vane source pin")
    return revision


def require_artifact(path: Path) -> None:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or not 256 < metadata.st_size <= MAX_ARTIFACT_BYTES:
        raise ValueError("signing input must be a bounded regular native artifact, not a link")
    with path.open("rb") as artifact:
        artifact.seek(-256, os.SEEK_END)
        if artifact.read() != b"\0" * 256:
            raise ValueError("signing input must have an unsigned DuckDB signature footer")


def require_key(contents: bytearray, profile: str) -> None:
    result = subprocess.run(
        ["/usr/bin/openssl", "pkey", "-pubout", "-outform", "DER", "-passin", "pass:"],
        input=contents,
        capture_output=True,
        env=SYSTEM_ENV,
        timeout=30,
        check=False,
    )
    if result.returncode or hashlib.sha256(result.stdout).hexdigest() != FINGERPRINTS[profile]:
        raise ValueError("signing key does not match the committed channel trust root")


def sign(args: argparse.Namespace, contents: bytearray) -> None:
    expected_manifest = "vane-extension-release.toml" if args.profile == "production" else "vane-extension.toml"
    if args.manifest != expected_manifest:
        raise ValueError("signing profile and committed manifest must select the same channel")
    revision = source_pin(args.manifest)
    actual = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=args.vane_source, env=SYSTEM_ENV, text=True
    ).strip()
    status = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=args.vane_source, env=SYSTEM_ENV
    )
    if actual != revision or status:
        raise ValueError("signer Vane checkout must match the clean committed pin")
    incoming = args.incoming.resolve()
    artifacts = incoming / "artifacts"
    if artifacts.is_symlink() or not artifacts.resolve(strict=True).is_relative_to(incoming):
        raise ValueError("signing input must remain within the immutable incoming data bundle")
    if {path.name for path in artifacts.iterdir()} != {f"{name}.duckdb_extension" for name in EXTENSION_NAMES}:
        raise ValueError("signer requires the complete fixed SQLite and DuckLake artifact set")
    for name in EXTENSION_NAMES:
        require_artifact(artifacts / f"{name}.duckdb_extension")
    require_key(contents, args.profile)
    temporary_root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    if temporary_root.is_relative_to(incoming):
        raise ValueError("signing key staging must be outside the incoming artifact")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError("signed artifact output must be empty")
    with tempfile.TemporaryDirectory(prefix="vane-private-signing-", dir=temporary_root) as value:
        key = Path(value) / "key.pem"
        try:
            descriptor = os.open(key, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as destination:
                destination.write(contents)
                destination.flush()
                os.fsync(destination.fileno())
            for name in EXTENSION_NAMES:
                subprocess.run(
                    [
                        "/usr/bin/python3",
                        "-I",
                        "-S",
                        str(args.vane_source.resolve() / "scripts/sign_test_dynamic_extension.py"),
                        "--private-key",
                        str(key),
                        str(artifacts / f"{name}.duckdb_extension"),
                        str(output / f"{name}.duckdb_extension"),
                    ],
                    env=SYSTEM_ENV,
                    check=True,
                    timeout=120,
                )
        finally:
            if key.exists():
                with key.open("r+b", buffering=0) as destination:
                    destination.write(b"\0" * key.stat().st_size)
                    os.fsync(destination.fileno())
                key.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("manifest", "sign"))
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--profile", choices=tuple(FINGERPRINTS))
    parser.add_argument("--vane-source", type=Path)
    parser.add_argument("--incoming", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.operation == "manifest":
        if args.github_output is None:
            parser.error("manifest requires --github-output")
        with args.github_output.open("a", encoding="utf-8") as destination:
            destination.write(f"vane_revision={source_pin(args.manifest)}\n")
        return 0
    contents = bytearray(os.environ.pop("VANE_SIGNING_PRIVATE_KEY", "").encode("utf-8"))
    try:
        if not contents or len(contents) > 64 * 1024:
            raise ValueError("isolated signer requires a bounded channel private key")
        if args.profile is None or args.vane_source is None or args.incoming is None or args.output is None:
            parser.error("sign requires --profile, --vane-source, --incoming and --output")
        sign(args, contents)
    finally:
        contents[:] = b"\0" * len(contents)
        contents.clear()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
