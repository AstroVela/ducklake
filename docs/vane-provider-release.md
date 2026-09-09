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
   interpreters, then prepare unsigned native data and licenses without keys.
   A separate protected signer signs that data, and a fresh no-secret job
   packages and verifies the matching DuckLake providers without rebuilding.
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
artifacts. Only the isolated signer receives it, removes it from the process
environment before invoking subprocesses, and erases its private temporary file
outside the downloaded data before the job uploads signed native files.

## Production release preparation

The same top-level `VaneExtension.yml` also offers `operation=release`;
`build-only` remains the default. Production preparation does not change
`vane-extension.toml`, the existing `0.2.0.dev612` runtime dependency, or any
already-published TestPyPI wheels.

Production instead uses `vane-extension-release.toml`. Its current exact Vane
pin, `033b549afcb498633fd6669b26c054c00363004e`, contains the production public
key but **is not a released runtime**. A release dispatch therefore fails at
the read-only version gate, before native dependency builds, signing, or
publication. First release a canonical non-development Vane version to PyPI,
then update this separate manifest to its exact source commit by reviewed PR.
The source must descend from the production-key commit, and all five matching
runtime wheels must exist on PyPI. There is no TestPyPI runtime fallback or
version substitution in the production lane.

After that prerequisite, a manual `release` dispatch on this repository's
protected `v1.5-variegata_vane` branch performs:

1. Download the exact runtime matrix from PyPI and build the native extension
   once in a job with no environment, secrets, or OIDC permission. Both the
   CI-test and TestPyPI test-key CMake options are explicitly disabled. Emit
   only unsigned native data and license texts. A protected isolated job
   signs the fixed native artifact once with trust identity `astrovela/vane`.
   It uses system Python with `-I -S`, the exact Vane stdlib signer, and system
   OpenSSL: no pip installation, native loading, or build subprocesses occur
   in the job holding the private key. The key's public DER SHA256 must equal
   `8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb`.
2. Package and verify the signed data in a fresh job without secrets or OIDC,
   using the original preparation/signing artifact IDs and exact indexed
   runtimes; do not rebuild native code. Validate source pins, exact dependencies, wheel sizes, and absent or
   byte-identical files on both indexes using the shared release CLI. Stage
   that candidate on TestPyPI with the existing publisher.
3. Verify the complete indexed filenames and hashes. Fresh local and
   two-worker Ray jobs install the runtime from PyPI and provider from
   TestPyPI, compare the provider bytes with the build artifact, run
   `pip check`, and execute the provider-backed SQL tests.
4. Wait for the protected `pypi` environment approval after both tests pass.
   Re-run shared `verify-promotion` in a read-only job with **no OIDC
   permission**. A separate approval-gated publisher has OIDC permission but
   executes only the pinned artifact-download and PyPI-publish actions. It
   downloads the **original assembly artifact ID**, never a mutable artifact
   name or a verifier-created artifact, and publishes those **same files**.
   A third read-only job verifies the indexed hashes. GitHub may request a
   second `pypi` approval for the minimal publisher after verification; keep
   required reviewers enabled for both jobs.
   There is no rebuild, re-signing, repackaging, or dependency rewriting after
   qualification. A retry may skip existing files only after their exact
   identities pass the shared gate.

Before enabling a real release, configure these external prerequisites:

- A `production-signing` GitHub environment restricted to the protected
  default branch, with required reviewers and the
  `VANE_EXTENSION_SIGNING_PRIVATE_KEY` secret. This is the production key,
  separate from the existing `testpypi` secret; keep it out of workflow inputs,
  logs, Git, and artifacts.
- A `pypi` GitHub environment restricted to that branch, with **required
  reviewers** and self-review prevention. Declaring an environment in YAML
  does not configure its approval protection; configure it before dispatch.
- A PyPI Trusted Publisher for project `vane-extension-ducklake`, owner
  `AstroVela`, repository `ducklake`, workflow `VaneExtension.yml`, environment
  `pypi`. The existing TestPyPI publisher remains unchanged.

This change does not configure environments or secrets, create tags, or
upload packages. No provider tag is required: the manually selected protected
branch commit and the reviewed exact Vane/CI-tools pins are the release inputs.

`build_vane_dynamic_wheel.py` retains a full build/sign/package mode only for
the public `ci-test` key. Both publishing profiles require `--prepare-only`,
which rejects key arguments. The isolated signer reads the selected manifest
from the committed repository tree, validates the exact official Vane pin,
and accepts only a regular unsigned `artifacts/ducklake.duckdb_extension`
bounded to 384 MiB. It does not trust builder-supplied source repository/ref
outputs or execute downloaded code. License files travel separately as data
under `licenses/ducklake/`; only signed native files leave the signer.

## Focused development checks

With Python 3.11 or newer:

```sh
git submodule update --init vane-extension-ci-tools
python -m pip install -r vane-extension-ci-tools/requirements-release.txt "PyYAML>=6.0"
python -I test/vane/test_vane_provider_release.py
python -I test/vane/test_vane_dynamic_wheel.py
python -I test/vane/test_vane_release_signing.py
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
  --vane-version 0.2.0.dev612 --channel testpypi-dev \
  --require-publishable-on testpypi
```

Source verification is read-only and rejects mismatched or dirty Vane/tools
checkouts. After publication, `verify-index` accepts the same source/config
flags plus `--index testpypi --provider ducklake --version <exact-provider-version>` and the
directory containing the five assembled provider wheels.
