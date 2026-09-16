#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Exercise the shared release CLI with this repository's SQLite and DuckLake dependency graph."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import tomllib
import unittest
import zipfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPOSITORY_ROOT / "vane-provider-release.toml"
VANE_VERSION = "0.2.0.dev612"
VERSIONS = {"sqlite_scanner": "0.2.0.0.612.2", "ducklake": "0.2.0.0.612.1"}
INTERPRETERS = ("cp310", "cp311", "cp312", "cp313", "cp314")
PLATFORM = "manylinux_2_28_x86_64"


def load_validator():
    path = REPOSITORY_ROOT / "vane-extension-ci-tools/scripts/vane_provider_release.py"
    specification = importlib.util.spec_from_file_location("vane_ducklake_release_validator", path)
    if specification is None or specification.loader is None:
        raise AssertionError(f"could not load {path}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def write_wheels(
    directory: Path,
    provider: str,
    *,
    vane_requirement: str | None = None,
    vane_version: str = VANE_VERSION,
    sqlite_requirement: str | None = None,
) -> list[Path]:
    distribution = f"vane_extension_{provider}"
    version = VERSIONS[provider]
    requirements = [vane_requirement or f"vane-ai==={vane_version}"]
    if provider == "ducklake":
        requirements.append(sqlite_requirement or f"vane-extension-sqlite-scanner==={VERSIONS['sqlite_scanner']}")
    metadata = (
        "Metadata-Version: 2.4\n"
        f"Name: vane-extension-{provider.replace('_', '-')}\n"
        f"Version: {version}\n" + "".join(f"Requires-Dist: {requirement}\n" for requirement in requirements) + "\n"
    )
    paths = []
    for interpreter in INTERPRETERS:
        path = directory / f"{distribution}-{version}-{interpreter}-none-{PLATFORM}.whl"
        with zipfile.ZipFile(path, "w") as wheel:
            wheel.writestr(f"{distribution}-{version}.dist-info/METADATA", metadata)
        paths.append(path)
    return paths


def source_arguments(directory: Path, *, manifest: str = "vane-extension.toml") -> list[str]:
    return [
        "--manifest",
        str(REPOSITORY_ROOT / manifest),
        "--extension-root",
        str(REPOSITORY_ROOT),
        "--vane-source",
        str(directory / "vane"),
        "--ci-tools-version",
        "a" * 40,
        "--config",
        str(CONFIG_PATH),
        "--directory",
        str(directory),
    ]


class ProviderReleaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.validator = load_validator()
        cls.config = cls.validator.load_config(CONFIG_PATH)

    @classmethod
    def tearDownClass(cls) -> None:
        sys.modules.pop(cls.validator.__name__, None)

    def test_configured_matrix_and_dependency_graph(self) -> None:
        self.assertEqual(self.config.interpreters, INTERPRETERS)
        self.assertEqual(self.config.platforms, (PLATFORM,))
        self.assertEqual(self.config.max_wheel_bytes, 100000000)
        self.assertEqual(
            [(provider.name, provider.distribution, provider.dependencies) for provider in self.config.providers],
            [
                ("sqlite_scanner", "vane-extension-sqlite-scanner", ()),
                ("ducklake", "vane-extension-ducklake", ("sqlite_scanner",)),
            ],
        )

    def test_complete_release_cli_outputs(self) -> None:
        # Generic matrix/index edge cases live in the shared tools repository.
        # This smoke test uses the actual consumer config and workflow outputs.
        with tempfile.TemporaryDirectory(prefix="vane-ducklake-release-") as value:
            directory = Path(value)
            for provider in VERSIONS:
                write_wheels(directory, provider)
            outputs = directory / "github-output"
            command = ["validate", *source_arguments(directory)]
            command += [
                "--vane-version",
                VANE_VERSION,
                "--channel",
                "testpypi-dev",
                "--github-output",
                str(outputs),
                "--require-publishable-on",
                "testpypi",
            ]
            output = io.StringIO()
            with (
                mock.patch.object(self.validator, "verify_sources") as verify,
                mock.patch.object(self.validator, "_request_json", return_value=(404, None)) as query,
                redirect_stdout(output),
            ):
                self.assertEqual(self.validator.main(command), 0)
            verify.assert_called_once_with(
                REPOSITORY_ROOT / "vane-extension.toml", REPOSITORY_ROOT, directory / "vane", "a" * 40
            )
            self.assertEqual(query.call_count, len(VERSIONS))
            expected = {
                "vane_version": VANE_VERSION,
                **{f"{name}_version": version for name, version in VERSIONS.items()},
            }
            self.assertEqual(json.loads(output.getvalue()), expected)
            self.assertEqual(dict(line.split("=", 1) for line in outputs.read_text().splitlines()), expected)

    def test_provider_requires_the_exact_vane_version(self) -> None:
        with tempfile.TemporaryDirectory(prefix="vane-ducklake-dependency-") as value:
            directory = Path(value)
            write_wheels(directory, "sqlite_scanner")
            for requirement in ("vane-ai>=0.2", "vane-ai===0.2.0.dev611"):
                with self.subTest(requirement=requirement):
                    write_wheels(directory, "ducklake", vane_requirement=requirement)
                    command = [
                        "validate",
                        *source_arguments(directory),
                        "--channel",
                        "testpypi-dev",
                        "--vane-version",
                        VANE_VERSION,
                    ]
                    with mock.patch.object(self.validator, "verify_sources"), redirect_stderr(io.StringIO()):
                        self.assertEqual(self.validator.main(command), 2)

    def test_ducklake_requires_the_exact_sqlite_provider(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            write_wheels(directory, "sqlite_scanner")
            for requirement in ("vane-extension-sqlite-scanner>=0.2", "vane-extension-sqlite-scanner===0.1"):
                with self.subTest(requirement=requirement):
                    write_wheels(directory, "ducklake", sqlite_requirement=requirement)
                    with self.assertRaisesRegex(self.validator.ReleaseValidationError, "exact"):
                        self.validator.validate_release(directory, VANE_VERSION, self.config, channel="testpypi-dev")

    def test_index_cli_accepts_each_assembled_provider_directory(self) -> None:
        # Verify the same complete artifact set that the upload job consumes.
        for provider, version in VERSIONS.items():
            with (
                self.subTest(provider=provider),
                tempfile.TemporaryDirectory(prefix="vane-ducklake-index-") as value,
            ):
                directory = Path(value)
                paths = write_wheels(directory, provider)
                document = {
                    "urls": [
                        {
                            "filename": path.name,
                            "packagetype": "bdist_wheel",
                            "digests": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
                            "yanked": False,
                        }
                        for path in paths
                    ]
                }
                command = ["verify-index", *source_arguments(directory)]
                command += [
                    "--index",
                    "testpypi",
                    "--provider",
                    provider,
                    "--version",
                    version,
                    "--attempts",
                    "1",
                    "--delay-seconds",
                    "0",
                ]
                with (
                    mock.patch.object(self.validator, "verify_sources") as verify,
                    mock.patch.object(self.validator, "_request_json", return_value=(200, document)) as query,
                ):
                    self.assertEqual(self.validator.main(command), 0)
                verify.assert_called_once_with(
                    REPOSITORY_ROOT / "vane-extension.toml", REPOSITORY_ROOT, directory / "vane", "a" * 40
                )
                query.assert_called_once_with(
                    f"https://test.pypi.org/pypi/vane-extension-{provider.replace('_', '-')}/{version}/json"
                )

    def test_release_rejects_the_existing_dev_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            write_wheels(directory, "ducklake")
            command = [
                "validate",
                *source_arguments(directory, manifest="vane-extension-release.toml"),
                "--channel",
                "release",
                "--vane-version",
                VANE_VERSION,
            ]
            with mock.patch.object(self.validator, "verify_sources"), redirect_stderr(io.StringIO()):
                self.assertEqual(self.validator.main(command), 2)

    def test_release_promotes_only_the_complete_identical_testpypi_set(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            directory = Path(value)
            paths = [path for provider in VERSIONS for path in write_wheels(directory, provider, vane_version="0.2.0")]
            document = {
                "urls": [
                    {
                        "filename": path.name,
                        "packagetype": "bdist_wheel",
                        "digests": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
                        "yanked": False,
                    }
                    for path in paths
                ]
            }
            command = [
                "verify-promotion",
                *source_arguments(directory, manifest="vane-extension-release.toml"),
                "--vane-version",
                "0.2.0",
                "--attempts",
                "1",
                "--delay-seconds",
                "0",
            ]

            def query(url):
                if url.startswith("https://test.pypi.org/"):
                    prefix = (
                        "vane_extension_sqlite_scanner-"
                        if "/vane-extension-sqlite-scanner/" in url
                        else "vane_extension_ducklake-"
                    )
                    return 200, {"urls": [entry for entry in document["urls"] if entry["filename"].startswith(prefix)]}
                self.assertTrue(url.startswith("https://pypi.org/"))
                return 404, None

            with (
                mock.patch.object(self.validator, "verify_sources") as verify,
                mock.patch.object(self.validator, "_request_json", side_effect=query),
            ):
                self.assertEqual(self.validator.main(command), 0)
                verify.assert_called_once_with(
                    REPOSITORY_ROOT / "vane-extension-release.toml",
                    REPOSITORY_ROOT,
                    directory / "vane",
                    "a" * 40,
                )
                document["urls"][0]["digests"]["sha256"] = "0" * 64
                with redirect_stderr(io.StringIO()):
                    self.assertEqual(self.validator.main(command), 2)

    def test_integration_source_pins(self) -> None:
        manifest = tomllib.loads((REPOSITORY_ROOT / "vane-extension.toml").read_text())
        self.assertEqual(manifest["schema_version"], 2)
        self.assertEqual(manifest["vane"]["repository"], "AstroVela/vane")
        self.assertEqual(manifest["vane"]["revision"], "4a85ae05d89b0194ac57f18bbfe22593cdec00c8")
        self.assertEqual(manifest["vcpkg"]["revision"], "84bab45d415d22042bd0b9081aea57f362da3f35")
        release_manifest = tomllib.loads((REPOSITORY_ROOT / "vane-extension-release.toml").read_text())
        self.assertEqual(release_manifest["vane"]["revision"], "4a85ae05d89b0194ac57f18bbfe22593cdec00c8")
        release_manifest["vane"]["revision"] = manifest["vane"]["revision"]
        self.assertEqual(release_manifest, manifest)
        entry = subprocess.check_output(
            ["git", "-C", str(REPOSITORY_ROOT), "ls-files", "--stage", "vane-extension-ci-tools"], text=True
        ).split()
        self.assertEqual(entry[0], "160000")
        actual = subprocess.check_output(
            ["git", "-C", str(REPOSITORY_ROOT / "vane-extension-ci-tools"), "rev-parse", "HEAD"], text=True
        ).strip()
        self.assertEqual(actual, entry[1])
        workflow = (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text()
        self.assertIn(f"_vane_extension_ci.yml@{actual}", workflow)
        self.assertIn(f"ci_tools_version: {actual}", workflow)
        self.assertNotIn("scripts/validate_vane_provider_release.py", workflow)

    def test_shared_release_gates_and_explicit_publication(self) -> None:
        workflow = (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text()
        self.assertEqual(workflow.count("scripts/vane_provider_release.py validate"), 2)
        self.assertEqual(workflow.count("scripts/vane_provider_release.py verify-index"), 4)
        self.assertIn("HEAD:vane-extension-ci-tools", workflow)
        self.assertIn("refs/heads/v1.5-variegata_vane", workflow)
        self.assertIn('test "$GITHUB_EVENT_NAME" = workflow_dispatch', workflow)
        self.assertIn("repository-url: https://test.pypi.org/legacy/", workflow)
        self.assertIn("skip-existing: true", workflow)
        self.assertEqual(workflow.count("--require-publishable-on testpypi"), 2)
        self.assertEqual(workflow.count("--require-publishable-on pypi"), 2)
        self.assertNotIn("--require-testpypi-publishable", workflow)

    def test_workflow_requires_both_qualifications_before_immutable_promotion(self) -> None:
        workflow = yaml.load(
            (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text(), Loader=yaml.BaseLoader
        )
        dispatch = workflow["on"]["workflow_dispatch"]["inputs"]["operation"]
        self.assertEqual(dispatch["default"], "build-only")
        self.assertEqual(dispatch["options"], ["build-only", "testpypi-dev", "release"])
        self.assertEqual(workflow["env"]["PIP_CONFIG_FILE"], "/dev/null")
        for name in ("PIP_EXTRA_INDEX_URL", "PIP_FIND_LINKS", "PIP_TRUSTED_HOST"):
            self.assertEqual(workflow["env"][name], "")
        jobs = workflow["jobs"]
        candidate = jobs["vane-prepare-provider"]
        self.assertIn("github.repository == 'AstroVela/ducklake'", candidate["if"])
        self.assertNotIn("environment", candidate)
        self.assertEqual(candidate["permissions"], {"contents": "read"})
        self.assertNotIn("secrets[", repr(candidate))
        candidate_steps = {step["name"]: index for index, step in enumerate(candidate["steps"])}
        self.assertLess(
            candidate_steps["Download every exact indexed Vane runtime before native work"],
            candidate_steps["Install native and wheel tooling"],
        )
        preflight = candidate["steps"][candidate_steps["Download every exact indexed Vane runtime before native work"]]
        self.assertIn("merge-base --is-ancestor", preflight["run"])
        self.assertIn("033b549afcb498633fd6669b26c054c00363004e", preflight["run"])
        self.assertIn("validate_vane_version(sys.argv[1], sys.argv[2])", preflight["run"])
        signer = jobs["vane-sign-provider"]
        self.assertEqual(signer["needs"], "vane-prepare-provider")
        self.assertEqual(signer["permissions"], {"contents": "read"})
        self.assertIn("production-signing", signer["environment"]["name"])
        for step in signer["steps"]:
            if "run" in step:
                self.assertTrue(step["run"].startswith("/usr/bin/python3 -I -S scripts/sign_vane_release.py "))
                self.assertNotIn("pip", step["run"])
                self.assertNotIn("build_vane_dynamic_wheel", step["run"])
            if "uses" in step:
                self.assertTrue(
                    any(
                        step["uses"].startswith(f"actions/{action}@")
                        for action in (
                            "checkout",
                            "download-artifact",
                            "upload-artifact",
                        )
                    )
                )
        package = jobs["vane-testpypi-wheels"]
        self.assertEqual(set(package["needs"]), {"vane-prepare-provider", "vane-sign-provider"})
        self.assertEqual(package["permissions"], {"contents": "read"})
        self.assertNotIn("environment", package)
        self.assertNotIn("secrets[", repr(package))
        self.assertNotIn("build_vane_dynamic_wheel.py", repr(package))
        promotion = jobs["verify-pypi-promotion"]
        self.assertEqual(promotion["if"], "inputs.operation == 'release'")
        self.assertEqual(promotion["environment"]["name"], "pypi")
        self.assertEqual(promotion["permissions"], {"contents": "read"})
        self.assertEqual(
            set(promotion["needs"]),
            {
                "assemble-testpypi-ducklake",
                "testpypi-smoke-ducklake-integration",
                "testpypi-ray-ducklake-integration",
            },
        )
        self.assertTrue(any("verify-promotion" in step.get("run", "") for step in promotion["steps"]))
        publisher = jobs["publish-pypi-ducklake"]
        self.assertEqual(set(publisher["needs"]), {"assemble-testpypi-ducklake", "verify-pypi-promotion"})
        self.assertEqual(publisher["environment"]["name"], "pypi")
        self.assertEqual(publisher["permissions"], {"contents": "read", "id-token": "write"})
        artifact_id = "${{ needs.assemble-testpypi-ducklake.outputs.distributions_artifact_id }}"
        self.assertEqual(len(publisher["steps"]), 2)
        download, publish = publisher["steps"]
        self.assertTrue(download["uses"].startswith("actions/download-artifact@"))
        self.assertEqual(download["with"]["artifact-ids"], artifact_id)
        self.assertNotIn("name", download["with"])
        self.assertTrue(publish["uses"].startswith("pypa/gh-action-pypi-publish@"))
        self.assertEqual(publish["with"]["packages-dir"], "dist")
        self.assertEqual(publish["with"]["repository-url"], "https://upload.pypi.org/legacy/")
        self.assertFalse(any("run" in step for step in publisher["steps"]))
        indexed = jobs["verify-pypi-ducklake"]
        self.assertEqual(indexed["permissions"], {"contents": "read"})
        self.assertIn("publish-pypi-ducklake", indexed["needs"])
        self.assertIn("--index pypi", indexed["steps"][-1]["run"])
        for job in (promotion, indexed):
            self.assertFalse(any("upload-artifact@" in step.get("uses", "") for step in job["steps"]))
            for step in job["steps"]:
                if "download-artifact@" in step.get("uses", ""):
                    self.assertEqual(step["with"]["artifact-ids"], artifact_id)
                    self.assertNotIn("name", step["with"])
        for job in jobs.values():
            for step in job.get("steps", []):
                if "actions/download-artifact@" in step.get("uses", ""):
                    self.assertIn("3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c", step["uses"])
                    self.assertEqual(step["with"]["digest-mismatch"], "error")
        for name in ("testpypi-smoke-ducklake-integration", "testpypi-ray-ducklake-integration"):
            scripts = "\n".join(step.get("run", "") for step in jobs[name]["steps"])
            self.assertIn('download_exact "vane-ai==$VANE_VERSION" "$VANE_RUNTIME_INDEX"', scripts)
            self.assertIn('cmp "${expected_ducklake[0]}" "${ducklake_wheels[0]}"', scripts)
            self.assertIn("astrovela/vane'", jobs[name]["env"]["VANE_EXPECTED_EXTENSION_TRUST_IDENTITY"])


if __name__ == "__main__":
    unittest.main()
