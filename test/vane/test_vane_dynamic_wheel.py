#!/usr/bin/env python3
"""Focused tests for DuckLake's native provider build adapter."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]


class DynamicWheelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = ROOT / "scripts/build_vane_dynamic_wheel.py"
        spec = importlib.util.spec_from_file_location("ducklake_builder", path)
        cls.builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.builder)

    def test_vcpkg_comes_from_the_integration_manifest(self) -> None:
        self.assertEqual(
            self.builder._vcpkg_revision(ROOT / "vane-extension.toml"),
            "84bab45d415d22042bd0b9081aea57f362da3f35",
        )
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            for manifest in (
                'schema_version = 1',
                'schema_version = 2\n[vcpkg]\nrepository = "fork/vcpkg"\nrevision = "' + "a" * 40 + '"',
                'schema_version = 2\n[vcpkg]\nrepository = "microsoft/vcpkg"\nrevision = "main"',
            ):
                (root / "vane-extension.toml").write_text(manifest)
                with self.subTest(manifest=manifest), self.assertRaises(self.builder.QualificationError):
                    self.builder._vcpkg_revision(root / "vane-extension.toml")

    def test_build_environment_selects_only_dynamic_ducklake(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            dependencies = root / "deps"
            for name in ("arrow/ArrowConfig.cmake", "arrowflight/ArrowFlightConfig.cmake"):
                config = dependencies / "x64-linux/share" / name
                config.parent.mkdir(parents=True, exist_ok=True)
                config.touch()
            environment = {
                "PATH": os.environ["PATH"],
                "CMAKE_ARGS": "-DDUCKLAKE_VANE_DISTRIBUTED=OFF",
                "DUCKDB_ICEBERG_DIRECTORY": "/unreviewed",
                "SKBUILD_BUILD_DIR": "/unreviewed",
                "SETUPTOOLS_SCM_PRETEND_VERSION": "9.9",
                "VANE_CMAKE_COMPILER_LAUNCHER": "ccache",
            }
            with mock.patch.dict(os.environ, environment, clear=True):
                result = self.builder._build_environment(
                    extension_root=ROOT,
                    build_directory=root,
                    vane_vcpkg_installed=dependencies,
                    vcpkg_toolchain=root / "vcpkg.cmake",
                    jobs=12,
                    signing_cmake_option="VANE_ENABLE_TEST_EXTENSION_SIGNING_KEY",
                )
            arguments = shlex.split(result["CMAKE_ARGS"])
            for argument in (
                "-DVANE_LOADABLE_EXTENSIONS=ducklake",
                "-DEXTENSION_STATIC_BUILD=ON",
                "-DDUCKLAKE_VANE_DISTRIBUTED=ON",
                "-DENABLE_EXTENSION_AUTOLOADING=OFF",
                "-DENABLE_EXTENSION_AUTOINSTALL=OFF",
                "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache",
                "-DVANE_ENABLE_TEST_EXTENSION_SIGNING_KEY=ON",
                "-DVANE_ENABLE_TESTPYPI_EXTENSION_SIGNING_KEY=OFF",
            ):
                self.assertIn(argument, arguments)
            self.assertNotIn("DUCKDB_ICEBERG_DIRECTORY", result)
            self.assertNotIn("SETUPTOOLS_SCM_PRETEND_VERSION", result)
            self.assertEqual(result["DUCKDB_DUCKLAKE_DIRECTORY"], str(ROOT))
            self.assertEqual(result["CMAKE_BUILD_PARALLEL_LEVEL"], "12")

    def test_production_disables_both_testing_trust_roots(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            with mock.patch.object(self.builder, "_require_file"):
                environment = self.builder._build_environment(
                    extension_root=ROOT,
                    build_directory=root,
                    vane_vcpkg_installed=root,
                    vcpkg_toolchain=root / "vcpkg.cmake",
                    jobs=12,
                    signing_cmake_option=self.builder.SIGNING_PROFILES["production"][1],
                )
            arguments = shlex.split(environment["CMAKE_ARGS"])
            self.assertIn("-DVANE_ENABLE_TEST_EXTENSION_SIGNING_KEY=OFF", arguments)
            self.assertIn("-DVANE_ENABLE_TESTPYPI_EXTENSION_SIGNING_KEY=OFF", arguments)
            self.assertEqual(self.builder.SIGNING_PROFILES["production"][0], "astrovela/vane")

    def test_production_signing_fingerprint_is_fail_closed(self) -> None:
        contents = bytearray(b"ephemeral-test-key")
        for returncode, public_der in ((1, b""), (0, b"wrong-public-key")):
            result = subprocess.CompletedProcess([], returncode, public_der, b"diagnostic")
            with (
                self.subTest(returncode=returncode),
                mock.patch.object(self.builder.subprocess, "run", return_value=result),
                self.assertRaisesRegex(self.builder.QualificationError, "reviewed astrovela/vane"),
            ):
                self.builder._verify_production_key(contents)
        result = subprocess.CompletedProcess([], 0, b"reviewed-public-key", b"")
        with (
            mock.patch.object(self.builder.subprocess, "run", return_value=result) as run,
            mock.patch.object(self.builder, "PRODUCTION_PUBLIC_KEY_SHA256", hashlib.sha256(result.stdout).hexdigest()),
        ):
            self.builder._verify_production_key(contents)
            self.assertEqual(run.call_args.kwargs["input"], contents)
            self.assertEqual(run.call_args.args[0], ("openssl", "pkey", "-pubout", "-outform", "DER"))

    def test_production_runtimes_reject_development_or_mixed_versions(self) -> None:
        def runtime(version):
            return Path("/runtime/python"), Path(f"vane_ai-{version}-cp312-cp312-manylinux_2_28_x86_64.whl")

        for versions in (("0.2.0.dev612",), ("0.2.0+local",), ("0.2",), ("0.2.0", "0.3.0"), ()):
            with self.subTest(versions=versions), self.assertRaises(self.builder.QualificationError):
                self.builder._require_production_runtimes(tuple(runtime(version) for version in versions))
        self.builder._require_production_runtimes((runtime("0.2.0"), runtime("0.2.0")))
        for name in ("not-a-wheel.whl", "duckdb-0.2.0-cp312-cp312-manylinux_2_28_x86_64.whl"):
            with self.subTest(name=name), self.assertRaises(self.builder.QualificationError):
                self.builder._require_production_runtimes(((Path("/runtime/python"), Path(name)),))

    def test_consumed_key_is_private_and_removed(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            path = Path(value) / "key.pem"
            path.write_bytes(b"test-key")
            path.chmod(0o600)
            contents = self.builder._read_signing_private_key(path, consume=True)
            self.assertEqual(contents, b"test-key")
            self.assertFalse(path.exists())
            self.builder._clear_key(contents)
            self.assertEqual(contents, bytearray())

    def test_unsafe_consumed_keys_are_not_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            path = root / "key.pem"
            path.write_bytes(b"test-key")
            path.chmod(0o644)
            link = root / "link.pem"
            link.symlink_to(path)
            for candidate in (path, link):
                with self.subTest(candidate=candidate), self.assertRaises(self.builder.QualificationError):
                    self.builder._read_signing_private_key(candidate, consume=True)
                self.assertEqual(path.read_bytes(), b"test-key")
            path.chmod(0o600)
            os.link(path, root / "hardlink.pem")
            with self.assertRaises(self.builder.QualificationError):
                self.builder._read_signing_private_key(path, consume=True)
            self.assertTrue(path.exists())

    def test_testpypi_cannot_package_an_unpublished_runtime(self) -> None:
        arguments = argparse.Namespace(
            jobs=12,
            package_local_runtime=True,
            runtime_python=[],
            runtime_wheel=[],
            signing_profile="testpypi",
            consume_signing_private_key=True,
        )
        for profile in ("testpypi", "production"):
            arguments.signing_profile = profile
            with mock.patch.object(self.builder, "_parse_arguments", return_value=arguments):
                with self.assertRaisesRegex(self.builder.QualificationError, "indexed runtimes"):
                    self.builder.main()

    def test_build_failure_consumes_and_clears_the_testpypi_key(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            key_path = root / "key.pem"
            key_path.write_bytes(b"test-key")
            key_path.chmod(0o600)
            arguments = argparse.Namespace(
                jobs=12,
                package_local_runtime=False,
                runtime_python=[Path("/runtime/python")],
                runtime_wheel=[Path("/runtime/vane.whl")],
                signing_profile="testpypi",
                consume_signing_private_key=True,
                signing_private_key=key_path,
                extension_root=ROOT,
                manifest=Path("vane-extension.toml"),
                vane_source=root,
                vane_revision="a" * 40,
                vane_vcpkg_installed=root,
                vcpkg_toolchain=root / "toolchain.cmake",
                build_directory=root / "build",
                output_directory=root / "dist",
            )
            contents = []
            read_key = self.builder._read_signing_private_key

            def consume_key(*args, **kwargs):
                result = read_key(*args, **kwargs)
                contents.append(result)
                return result

            def run(command, **kwargs):
                if "identity" in command or "verify-ci-tools" in command:
                    self.assertEqual(command[command.index("--manifest") + 1], str(ROOT / arguments.manifest))
                if "build" in command:
                    self.assertFalse(key_path.exists())
                    self.assertEqual(contents[0], b"test-key")
                    raise RuntimeError("simulated build failure")

            with (
                mock.patch.object(self.builder, "_parse_arguments", return_value=arguments),
                mock.patch.object(self.builder, "_require_vcpkg_toolchain", return_value=root / "toolchain.cmake"),
                mock.patch.object(self.builder, "_require_git_revision"),
                mock.patch.object(self.builder, "_require_file", side_effect=lambda path, description: path),
                mock.patch.object(self.builder, "_capture", return_value="b" * 40),
                mock.patch.object(self.builder, "_build_environment", return_value={}),
                mock.patch.object(self.builder, "_platform_tag", return_value="manylinux_2_28_x86_64"),
                mock.patch.object(self.builder.os, "access", return_value=True),
                mock.patch.object(self.builder, "_read_signing_private_key", side_effect=consume_key),
                mock.patch.object(self.builder, "_run", side_effect=run),
                self.assertRaisesRegex(RuntimeError, "simulated build failure"),
            ):
                self.builder.main()
            self.assertEqual(contents, [bytearray()])
            self.assertFalse(key_path.exists())

    def test_toolchain_must_be_rooted_at_its_git_checkout(self) -> None:
        toolchain = Path("/repository/nested/scripts/buildsystems/vcpkg.cmake")
        with (
            mock.patch.object(self.builder, "_require_file", return_value=toolchain),
            mock.patch.object(self.builder, "_capture", return_value="/repository"),
            self.assertRaisesRegex(self.builder.QualificationError, "not rooted"),
        ):
            self.builder._require_vcpkg_toolchain(toolchain, "a" * 40)

    def test_artifact_rejects_external_libraries_and_duckdb_symbols(self) -> None:
        artifact = Path("/tmp/ducklake.duckdb_extension")
        for dynamic, symbols in (
            ("Shared library: [libroaring.so]", ""),
            ("(RUNPATH) [/tmp/build]", ""),
            ("Shared library: [libc.so.6]", "U duckdb::ParquetMultiFileInfo::SerializeBindData()"),
        ):
            with self.subTest(dynamic=dynamic, symbols=symbols):
                with mock.patch.object(self.builder, "_capture", side_effect=[dynamic, symbols]):
                    with self.assertRaises(self.builder.QualificationError):
                        self.builder._require_no_undefined_duckdb_symbols(artifact)
        with mock.patch.object(self.builder, "_capture", side_effect=["Shared library: [libc.so.6]", ""]):
            self.builder._require_no_undefined_duckdb_symbols(artifact)

    def test_reviewed_license_closure_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            share = Path(value)
            for name in self.builder.EXPECTED_VCPKG_LICENSE_COMPONENTS:
                path = share / name / "copyright"
                path.parent.mkdir()
                path.write_text(f"{name} license")
            bundle = self.builder._render_vcpkg_license_bundle(ROOT / "vane-extension.toml", share)
            self.assertIn("roaring license", bundle)
            extra = share / "unexpected" / "copyright"
            extra.parent.mkdir()
            extra.write_text("unknown license")
            with self.assertRaisesRegex(self.builder.QualificationError, "unexpected"):
                self.builder._render_vcpkg_license_bundle(ROOT / "vane-extension.toml", share)


if __name__ == "__main__":
    unittest.main()
