#!/usr/bin/env python3
"""Regression checks for the isolated signing and no-secret packaging boundary."""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]


def load_script(name):
    specification = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


signer = load_script("sign_vane_release")
packager = load_script("package_prepared_vane_wheel")


class SigningTest(unittest.TestCase):
    def test_signer_imports_only_stdlib(self):
        tree = ast.parse((ROOT / "scripts/sign_vane_release.py").read_text())
        for node in ast.walk(tree):
            names = [alias.name for alias in node.names] if isinstance(node, ast.Import) else []
            if isinstance(node, ast.ImportFrom):
                names.append(node.module)
            for name in names:
                self.assertIn(name.split(".")[0], sys.stdlib_module_names)

    def test_signer_uses_the_committed_official_source_pin(self):
        contents = (
            'schema_version = 2\nname = "ducklake"\n[vane]\nrepository = "AstroVela/vane"\nrevision = "'
            + "a" * 40
            + '"\n'
        )
        with mock.patch.object(signer.subprocess, "check_output", return_value=contents.encode()) as read:
            self.assertEqual(signer.source_pin("vane-extension-release.toml"), "a" * 40)
        self.assertEqual(read.call_args.args[0], ["git", "show", "HEAD:vane-extension-release.toml"])
        with mock.patch.object(
            signer.subprocess, "check_output", return_value=contents.replace("AstroVela/vane", "fork/vane").encode()
        ):
            with self.assertRaises(ValueError):
                signer.source_pin("vane-extension-release.toml")
        with self.assertRaises(ValueError):
            signer.source_pin("unreviewed.toml")

    def test_only_bounded_unsigned_regular_native_data_is_signable(self):
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            artifact = root / "ducklake.duckdb_extension"
            artifact.write_bytes(b"native payload" + b"\0" * 256)
            signer.require_artifact(artifact)
            link = root / "link"
            link.symlink_to(artifact)
            with self.assertRaises(ValueError):
                signer.require_artifact(link)
            os.link(artifact, root / "hardlink")
            with self.assertRaises(ValueError):
                signer.require_artifact(artifact)
            (root / "hardlink").unlink()
            artifact.write_bytes(b"native payload" + b"signed" * 50)
            with self.assertRaises(ValueError):
                signer.require_artifact(artifact)
            with artifact.open("wb") as output:
                output.truncate(signer.MAX_ARTIFACT_BYTES + 1)
            with self.assertRaises(ValueError):
                signer.require_artifact(artifact)

    def test_key_fingerprints_are_separate_and_fail_closed(self):
        self.assertEqual(
            signer.FINGERPRINTS["production"], "8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb"
        )
        self.assertEqual(
            signer.FINGERPRINTS["testpypi"], "53779fb8f9c97e9dec9c66ff838839eb234d1a64d4b105671304820e627b5e32"
        )
        contents = bytearray(b"not a production private key")
        for returncode, public in ((1, b""), (0, b"wrong DER")):
            with mock.patch.object(
                signer.subprocess, "run", return_value=subprocess.CompletedProcess([], returncode, public, b"")
            ):
                for profile in signer.FINGERPRINTS:
                    with self.assertRaises(ValueError):
                        signer.require_key(contents, profile)
        public = b"reviewed public DER fixture"
        with (
            mock.patch.dict(signer.FINGERPRINTS, {"production": hashlib.sha256(public).hexdigest()}),
            mock.patch.object(
                signer.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, public, b"")
            ) as run,
        ):
            signer.require_key(contents, "production")
        self.assertIs(run.call_args.kwargs["input"], contents)
        self.assertEqual(run.call_args.kwargs["env"], signer.SYSTEM_ENV)

    def test_key_is_unset_before_subprocesses_and_erased_after_signing_failure(self):
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            incoming = root / "incoming/artifacts"
            incoming.mkdir(parents=True)
            (incoming / "ducklake.duckdb_extension").write_bytes(b"native payload" + b"\0" * 256)
            vane = root / "vane"
            vane.mkdir()
            observed = []

            def fail(command, **kwargs):
                self.assertNotIn("VANE_SIGNING_PRIVATE_KEY", os.environ)
                self.assertEqual(command[:3], ["/usr/bin/python3", "-I", "-S"])
                self.assertEqual(kwargs["env"], signer.SYSTEM_ENV)
                key = Path(command[command.index("--private-key") + 1])
                self.assertEqual(key.stat().st_mode & 0o777, 0o600)
                self.assertFalse(key.is_relative_to(root / "incoming"))
                self.assertEqual(key.read_bytes(), b"temporary fixture key")
                observed.append(key)
                raise RuntimeError("signer subprocess failed")

            args = [
                "signer",
                "sign",
                "--manifest",
                "vane-extension-release.toml",
                "--profile",
                "production",
                "--vane-source",
                str(vane),
                "--incoming",
                str(root / "incoming"),
                "--output",
                str(root / "signed"),
            ]
            with (
                mock.patch.object(sys, "argv", args),
                mock.patch.dict(
                    os.environ, {"RUNNER_TEMP": str(root), "VANE_SIGNING_PRIVATE_KEY": "temporary fixture key"}
                ),
                mock.patch.object(signer, "source_pin", return_value="a" * 40),
                mock.patch.object(signer, "require_key"),
                mock.patch.object(signer.subprocess, "check_output", side_effect=["a" * 40, b""]),
                mock.patch.object(signer.subprocess, "run", side_effect=fail),
                self.assertRaisesRegex(RuntimeError, "signer subprocess failed"),
            ):
                signer.main()
            self.assertEqual(len(observed), 1)
            self.assertFalse(observed[0].exists())

    def test_packaging_reuses_verifiers_without_native_build_or_signing(self):
        builder = packager.load_builder()
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            unsigned = root / "prepared/artifacts/ducklake.duckdb_extension"
            unsigned.parent.mkdir(parents=True)
            unsigned.write_bytes(b"native payload" + b"\0" * 256)
            signed = root / "signed/ducklake.duckdb_extension"
            signed.parent.mkdir()
            signed.write_bytes(b"native payload" + b"s" * 256)
            licenses = root / "prepared/licenses/ducklake"
            licenses.mkdir(parents=True)
            for name in builder.LICENSE_NAMES:
                (licenses / name).write_text("license data")
            wheel = root / "provider.whl"
            wheel.write_bytes(b"verified wheel fixture")
            runtime = root / "vane.whl"
            runtime.touch()
            args = [
                "package",
                "--vane-source",
                str(root),
                "--vane-revision",
                "a" * 40,
                "--profile",
                "testpypi",
                "--prepared",
                str(root / "prepared"),
                "--signed",
                str(root / "signed"),
                "--output",
                str(root / "dist"),
                "--runtime-python",
                sys.executable,
                "--runtime-wheel",
                str(runtime),
            ]
            with (
                mock.patch.object(sys, "argv", args),
                mock.patch.object(packager, "load_builder", return_value=builder),
                mock.patch.object(builder, "_require_git_revision"),
                mock.patch.object(builder, "_builder_python", return_value=(mock.Mock(), Path(sys.executable))),
                mock.patch.object(builder, "_build_provider_wheel", return_value=wheel),
                mock.patch.object(builder, "_run") as run,
            ):
                self.assertEqual(packager.main(), 0)
            self.assertEqual(run.call_count, 1)
            self.assertIn("verify_extension_wheel.py", repr(run.call_args))
            self.assertEqual((root / "dist/provider.whl").read_bytes(), wheel.read_bytes())


if __name__ == "__main__":
    unittest.main()
