# Vane DuckLake provider releases

`vane-extension-ducklake` packages DuckLake as a signed dynamic Vane provider,
separate from the `vane-ai` runtime. The development build targets the exact
Vane source recorded in `vane-extension.toml`, corresponding to
`vane-ai==0.2.0`. Each provider wheel requires that exact runtime version;
the wheel is not interchangeable with arbitrary Vane or upstream DuckDB builds.

The existing native and statically linked Vane-wheel integration lanes remain
in place. The new provider lane builds an independent dynamic extension and
qualifies it with default Ray CRUD and two-worker scans. The delivery includes
`vane-extension-sqlite-scanner`, built from Vane's pinned DuckDB SQLite source
`f79b1db7d7730b18d0f8400d3650ffa6b45168d8`. DuckLake's descriptor and Python
requirements pin that exact provider. Loading DuckLake loads SQLite first;
neither extension is statically linked into the base Vane wheel.

The tests attach `ducklake:sqlite:<absolute metadata.sqlite path>` and use
filesystem-backed data. SQLite lets the client and Ray driver share a catalog
across processes on the same host. This local two-worker-node qualification
does not certify a SQLite catalog shared across multiple physical hosts.

## Ownership and release contract

The repository owns `scripts/build_vane_dynamic_wheel.py`, the DuckLake-native
build configuration, license collection, and provider-backed SQL tests. The
exact Vane checkout's wheel builder and verifier own descriptor, SourceID,
platform, signature, native dependency, license, and archive-safety checks.

Release-matrix and TestPyPI index validation are shared through the pinned
`vane-extension-ci-tools` submodule, not copied into this repository.
`vane-provider-release.toml` declares:

- CPython 3.10 through 3.14, `none` ABI, `manylinux_2_28_x86_64`;
- one `vane-extension-ducklake` and one `vane-extension-sqlite-scanner` wheel per interpreter;
- exact `vane-ai` requirements for both, plus DuckLake's exact SQLite provider dependency;
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
`test/vane/test_vane_dynamic_ducklake.py --smoke` for CRUD and the same script
without `--smoke` for two-worker coverage. All public integration tests leave
`VANE_RUNNER` unset, avoid runner-selection APIs, and verify the default Ray
runner. The statically linked suite also uses the default Ray runner.

Build-only CI enables the public CI test key in its locally built runtime and
packages a matching runtime/provider set. These are test artifacts even though
the runtime reports `0.2.0`; do not mix them with the PyPI runtime or publish them.

Use `build-only` for PRs and `release` for production. With the stable pin,
`testpypi-dev` deliberately rejects `0.2.0`; a future development publication
requires a separately reviewed pin to its exact TestPyPI runtime. That channel
uses a manual `Vane extension` dispatch with `operation=testpypi-dev` on
`v1.5-variegata_vane`:

1. Download the exact indexed `vane-ai` runtime wheels for all five CPython
   interpreters, then prepare unsigned native data and licenses without keys.
   A separate protected signer signs that data, and a fresh no-secret job
   packages and verifies the matching SQLite and DuckLake providers without rebuilding.
2. Use the shared CLI to validate the complete matrix, exact dependencies,
   upload sizes, and absent or byte-identical existing TestPyPI files.
3. Revalidate from the assembled artifacts, create checksums, SBOM, provenance,
   and Sigstore evidence, then publish through Trusted Publishing.
4. Compare all ten indexed filenames and SHA256 hashes with the exact upload
   artifacts. Partial or conflicting indexed sets fail verification.
5. Download the indexed runtime/provider graph in fresh default Ray smoke and two-worker
   Ray jobs. Compare the provider bytes with the upload artifact, install the
   exact graph, run `pip check`, and exercise the dynamic-provider SQL tests.

Final upload remains in this repository's top-level `VaneExtension.yml`.
There is no central publishing service, runtime download, compatibility
selection, or fallback path added by this integration. No packages are
published merely by merging its implementation PR.

## Development TestPyPI setup

For future development publications with a matching development runtime, configure:

- a protected GitHub environment named `testpypi` in `AstroVela/ducklake`;
- its `VANE_TESTPYPI_EXTENSION_SIGNING_PRIVATE_KEY` secret, containing the
  existing private key for trust identity `astrovela/vane-testpypi`; that
  development runtime must embed the matching public key. Vane v0.2.0 does not;
- TestPyPI Trusted Publishers for projects `vane-extension-ducklake` and
  `vane-extension-sqlite-scanner`, both with
  owner `AstroVela`, repository `ducklake`, workflow `VaneExtension.yml`, and
  environment `testpypi`.

These repository/project settings are external prerequisites, not created by
this source change. Keep the private key out of Git, workflow inputs, logs, and
artifacts. Only the isolated signer receives it, removes it from the process
environment before invoking subprocesses, and erases its private temporary file
outside the downloaded data before the job uploads signed native files.

## Production release

The same top-level `VaneExtension.yml` also offers `operation=release`;
`build-only` remains the default. Both manifests pin Vane v0.2.0 at
`79049f382ba6ee79d035c09cc8b5d3538e5bbe6a`.

Production uses `vane-extension-release.toml`, exact PyPI runtime wheels and
the production signer. The source must descend from the production-key commit,
and all five matching runtime wheels must exist on PyPI. There is no TestPyPI
runtime fallback or version substitution. Updating the pin does not publish
providers or establish production qualification.

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
3. Verify the complete indexed filenames and hashes. Fresh default Ray smoke and
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
- PyPI Trusted Publishers for projects `vane-extension-ducklake` and
  `vane-extension-sqlite-scanner`, both with owner `AstroVela`, repository
  `ducklake`, workflow `VaneExtension.yml`, environment `pypi`. Configure both
  projects on TestPyPI with environment `testpypi` as described above.

This change does not configure environments or secrets, create tags, or
upload packages. No provider tag is required: the manually selected protected
branch commit and the reviewed exact Vane/CI-tools pins are the release inputs.

`build_vane_dynamic_wheel.py` retains a full build/sign/package mode only for
the public `ci-test` key. Both publishing profiles require `--prepare-only`,
which rejects key arguments. The isolated signer reads the selected manifest
from the committed repository tree, validates the exact official Vane pin,
and requires exactly two regular unsigned files under `artifacts/`:
`sqlite_scanner.duckdb_extension` and `ducklake.duckdb_extension`, each bounded
to 384 MiB. It does not trust builder-supplied source repository/ref outputs
or execute downloaded code. License files travel separately as data under
`licenses/sqlite_scanner/` and `licenses/ducklake/`; only signed native files
leave the signer.

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
  --manifest vane-extension-release.toml --extension-root . \
  --vane-source ../vane \
  --ci-tools-version "$(git rev-parse HEAD:vane-extension-ci-tools)" \
  --config vane-provider-release.toml \
  --directory build/vane-testpypi-wheel-dist \
  --vane-version 0.2.0 --channel release \
  --require-publishable-on testpypi --require-publishable-on pypi
```

Source verification is read-only and rejects mismatched or dirty Vane/tools
checkouts. After publication, `verify-index` accepts the same source/config
flags plus `--index testpypi --provider <name> --version <exact-provider-version>`.
Run it for both `sqlite_scanner` and `ducklake`, using the directory containing
the ten assembled provider wheels (five interpreters per provider).

## Default Ray qualification

Both manifests pin Vane v0.2.0,
`79049f382ba6ee79d035c09cc8b5d3538e5bbe6a`. Build-only tests qualify the
test-key runtime/provider set; production qualification uses the PyPI runtime.

The ordered INSERT regression covers zero, 256 and 8193 rows, validates every
row after publication, and checks empty writes publish no files. The
single-file scan fixtures retain VALUES batches to control file boundaries.
SQL statements, Relations, mutations and readback use the default Ray runner.
Test-owned clusters reserve capacity for concurrent writes without changing
Vane's runner selection. Controlled legacy mapping and inlined-delete fixtures
use PyArrow and isolated Python `sqlite3` processes. SQLite inspection and
failure-injection triggers use the same independent fixture path, without
loading a local Vane runner. These fixtures qualify scans and write recovery;
they do not imply Ray support for DuckLake maintenance `CALL` statements.

DuckLake metadata functions share a portable completed-row binding. Their rows
are copied and serialized once per bound plan, including `ducklake_options()`;
Ray runs one metadata fragment and retries retain the same rows. Creating a new
query observes current catalog metadata. This does not declare live catalog
objects to be worker-safe or add any execution fallback.

The native MinIO fixture pins both upstream Quay images by release tag and
digest. Readiness and bucket initialization have finite timeouts; failures stop
the job, and cleanup removes only this fixture project's containers and volume.
The SQLite-backed Ray tests remain a same-host, two-worker qualification.

## SQLite dependency checks

The signer accepts exactly two unsigned artifacts and signs both with the
selected channel key. Packaging checks both signed payloads against preparation,
collects the SQLite extension's MIT license and SQLite's blessing notice, and
passes the SQLite wheel to Vane's dependency-aware builder and verifier.
Assembly, index byte checks, and immutable promotion cover the complete graph;
the existing `testpypi` and `pypi` publishing environments serve both projects.
Register the additional SQLite project publisher before any manual publication.
