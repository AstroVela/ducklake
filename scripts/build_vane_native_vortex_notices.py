#!/usr/bin/env python3
"""Stage the notices for the native DuckLake and Vortex provider wheels."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import shutil
import subprocess


def load_packager(source: Path, name: str):
    path = source / "scripts/build_vane_dynamic_wheel.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    packager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(packager)
    return packager


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vane-source", type=Path, required=True)
    parser.add_argument("--vortex-wrapper", type=Path, required=True)
    parser.add_argument("--ducklake-vcpkg-share", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1]
    vane = args.vane_source.resolve(strict=True)
    wrapper = args.vortex_wrapper.resolve(strict=True)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)

    ducklake = load_packager(source, "ducklake_packager")
    vortex = load_packager(wrapper, "vortex_packager")
    (output / "DuckDB-static-engine-licenses.txt").write_text(
        ducklake._render_duckdb_license_bundle(vane), encoding="utf-8"
    )
    (output / "DuckLake-vcpkg-licenses.txt").write_text(
        ducklake._render_vcpkg_license_bundle(source / "vane-native-vortex.toml", args.ducklake_vcpkg_share),
        encoding="utf-8",
    )
    shutil.copyfile(vane / "LICENSE", output / "Vane-Apache-2.0.txt")
    shutil.copyfile(vane / "NOTICE", output / "Vane-NOTICE.txt")
    sysroot = Path(subprocess.check_output(["rustc", "--print", "sysroot"], text=True).strip())
    shutil.copyfile(sysroot / "share/doc/rust/COPYRIGHT-library.html", output / "Rust-stdlib-licenses.html")

    # The native wrapper's Rust sources declare Apache-2.0 in their SPDX headers.
    # Its manifest omits the license field. Use the pinned checkout's Apache text.
    config = wrapper / "about.toml"
    vortex_config = output / "vortex-cargo-about.toml"
    vortex_config.write_text(
        config.read_text(encoding="utf-8")
        + """
[vortex-extension.clarify]
license = "Apache-2.0"

[[vortex-extension.clarify.files]]
path = "../vortex-extension-vane/LICENSE"
checksum = "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"
""",
        encoding="utf-8",
    )
    expressions = {
        "vortex": vortex.LICENSE_EXPRESSION,
        "ducklake": " AND ".join(
            dict.fromkeys(ducklake.LICENSE_EXPRESSION.split(" AND ") + vortex.LICENSE_EXPRESSION.split(" AND "))
        ),
    }
    manifests = {
        "vortex": (wrapper / "vortex-extension/Cargo.toml", vortex_config),
        "ducklake": (source / "src/vortex_query/Cargo.toml", config),
    }
    for name, (manifest, crate_config) in manifests.items():
        notice = output / f"{name}-rust-licenses.txt"
        subprocess.run(
            [
                "cargo",
                "about",
                "generate",
                "--locked",
                "--fail",
                "--config",
                str(crate_config),
                "--manifest-path",
                str(manifest),
                "--output-file",
                str(notice),
                str(wrapper / "about.hbs"),
            ],
            cwd=manifest.parent,
            check=True,
        )
        if notice.stat().st_size == 0:
            raise RuntimeError(f"cargo-about produced an empty {name} notice")
        (output / f"{name}-license-expression.txt").write_text(expressions[name] + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
