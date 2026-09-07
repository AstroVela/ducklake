# Vane DuckLake provider releases

`vane-extension-ducklake` packages DuckLake as a signed dynamic Vane provider,
separate from the `vane-ai` runtime. The initial candidate targets the exact
Vane source recorded in `vane-extension.toml`, corresponding to
`vane-ai==0.2.0.dev612`. Each provider wheel requires that exact runtime version;
the wheel is not interchangeable with arbitrary Vane or upstream DuckDB builds.

The existing native and statically linked Vane-wheel integration lanes remain
in place. The new provider lane builds an independent dynamic extension and
qualifies it with local execution and two Ray workers. Initial dynamic
qualification uses DuckLake's DuckDB metadata catalog and filesystem-backed
data, with no SQLite or PostgreSQL provider package dependency.

## Ownership and release contract

The repository owns `scripts/build_vane_dynamic_wheel.py`, the DuckLake-native
build configuration, license collection, and provider-backed SQL tests. The
exact Vane checkout's wheel builder and verifier own descriptor, SourceID,
platform, signature, native dependency, license, and archive-safety checks.

Release-matrix and TestPyPI index validation are shared through the pinned
`vane-extension-ci-tools` submodule, not copied into this repository.
`vane-provider-release.toml` declares:

- CPython 3.10 through 3.14, `none` ABI, `manylinux_2_28_x86_64`;
- one `vane-extension-ducklake` wheel per interpreter;
- an exact `vane-ai` dependency and no other provider dependencies;
- a 100,000,000-byte per-wheel upload budget, independent of Vane's native
  artifact safety limits.

The shared release CLI runs under Python 3.11 or newer, independently of the
wheel interpreters. The workflow uses Python 3.12 for release tooling and pins
every Vane/CI-tools/vcpkg source revision explicitly. The native manifest's
vcpkg revision remains `84bab45d415d22042bd0b9081aea57f362da3f35`.

## CI and publication

Normal PR and branch CI do not publish packages. They exercise lightweight
packaging/release tests and build a CI-test-signed provider plus its exact Vane
runtime wheel. Separate clean jobs install those artifacts and run
`test/vane/test_vane_dynamic_ducklake.py` under `VANE_RUNNER=local-fast` and
`VANE_RUNNER=ray`. The original statically linked tests remain independent.

Publication requires a manual `Vane extension` workflow dispatch with
`operation=testpypi-dev` on `v1.5-variegata_vane`:

1. Download the exact indexed `vane-ai` runtime wheels for all five CPython
   interpreters, then build, sign, and verify the matching DuckLake providers.
2. Use the shared CLI to validate the complete matrix, exact dependencies,
   upload sizes, and absent or byte-identical existing TestPyPI files.
3. Revalidate from the assembled artifacts, create checksums, SBOM, provenance,
   and Sigstore evidence, then publish through Trusted Publishing.
4. Compare the five indexed filenames and SHA256 hashes with the exact upload
   artifacts. Partial or conflicting indexed sets fail verification.
5. Download the indexed runtime/provider graph in fresh local and two-worker
   Ray jobs. Compare the provider bytes with the upload artifact, install the
   exact graph, run `pip check`, and exercise the dynamic-provider SQL tests.

Final upload remains in this repository's top-level `VaneExtension.yml`.
There is no central publishing service, runtime download, compatibility
selection, or fallback path added by this integration. No packages are
published merely by merging its implementation PR.

## First TestPyPI setup

Before the first manual publication, configure:

- a protected GitHub environment named `testpypi` in `AstroVela/ducklake`;
- its `VANE_TESTPYPI_EXTENSION_SIGNING_PRIVATE_KEY` secret, containing the
  existing private key for trust identity `astrovela/vane-testpypi`; the pinned
  Vane candidate already embeds the matching public key;
- a TestPyPI Trusted Publisher for project `vane-extension-ducklake`, with
  owner `AstroVela`, repository `ducklake`, workflow `VaneExtension.yml`, and
  environment `testpypi`.

These repository/project settings are external prerequisites, not created by
this source change. Keep the private key out of Git, workflow inputs, logs, and
artifacts. The workflow provides it only to the signing build step and consumes
the temporary private-key file after reading it.

## Focused development checks

With Python 3.11 or newer:

```sh
git submodule update --init vane-extension-ci-tools
python -m pip install -r vane-extension-ci-tools/requirements-release.txt
python -I test/vane/test_vane_provider_release.py
python -I test/vane/test_vane_dynamic_wheel.py
```

To validate an already-built candidate matrix, use the exact clean Vane source
checkout selected by the manifest:

```sh
python -I vane-extension-ci-tools/scripts/vane_provider_release.py validate \
  --manifest vane-extension.toml --extension-root . \
  --vane-source ../vane \
  --ci-tools-version "$(git rev-parse HEAD:vane-extension-ci-tools)" \
  --config vane-provider-release.toml \
  --directory build/vane-testpypi-wheel-dist \
  --vane-version 0.2.0.dev612 --require-testpypi-publishable
```

Source verification is read-only and rejects mismatched or dirty Vane/tools
checkouts. After publication, `verify-index` accepts the same source/config
flags plus `--provider ducklake --version <exact-provider-version>` and the
directory containing the five assembled provider wheels.
