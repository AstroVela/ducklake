#!/usr/bin/env bash
set -euo pipefail

if (($# != 1)); then
    printf '%s\n' 'Usage: VORTEX_SOURCE_DIR=... ZSTD_STATIC_LIBRARY=... ZSTD_INCLUDE_DIR=... scripts/build_vane_vortex_native_sdk.sh BUILD_DIR' >&2
    exit 2
fi
vortex_source=${VORTEX_SOURCE_DIR:?Set the pinned Vortex source directory}
zstd_library=${ZSTD_STATIC_LIBRARY:?Set the PIC static zstd archive}
zstd_include=${ZSTD_INCLUDE_DIR:?Set the qualified zstd include directory}
[[ $(git -C "$vortex_source" rev-parse HEAD) == 65dab44f86495d35bb46f69c8ec5cfc7a3544c1b ]]
git -C "$vortex_source" diff --quiet HEAD
[[ "$zstd_library" == /*.a && -f "$zstd_library" && -f "$zstd_include/zstd.h" ]]
mkdir -p "$1"
build_root=$(cd "$1" && pwd)
export CMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL:-2}
export PYTHONDONTWRITEBYTECODE=1

download() {
    local filename=$1 expected=$2
    if [[ ! -f "$build_root/$filename" ]]; then
        curl --fail --location --silent --show-error \
            "https://github.com/llvm/llvm-project/releases/download/llvmorg-18.1.8/$filename" \
            --output "$build_root/$filename.partial"
        mv "$build_root/$filename.partial" "$build_root/$filename"
    fi
    printf '%s  %s\n' "$expected" "$build_root/$filename" | sha256sum --check
}
download openmp-18.1.8.src.tar.xz 60ed57245e73894e4a2a89b15889f367bd906abfe6d3f92e1718223d4b496150
download cmake-18.1.8.src.tar.xz 59badef592dd34893cd319d42b323aaa990b452d05c7180ff20f23ab1b41e837

python3 - "$build_root" <<'PY'
import hashlib
from pathlib import Path
import sys
import tarfile

root = Path(sys.argv[1])
sources = root / "llvm"
stamp = root / "llvm-source.sha256"
if sources.exists() and not stamp.exists():
    raise SystemExit("Unstamped LLVM source; use a fresh build directory")
if not sources.exists():
    sources.mkdir()
    for name in ["openmp", "cmake"]:
        with tarfile.open(root / f"{name}-18.1.8.src.tar.xz") as archive:
            archive.extractall(sources, filter="data")
    (sources / "cmake-18.1.8.src").rename(sources / "cmake")

digest = hashlib.sha256()
for path in sorted(sources.rglob("*")):
    if path.is_file():
        name = str(path.relative_to(sources)).encode()
        contents = path.read_bytes()
        digest.update(len(name).to_bytes(8, "little") + name)
        digest.update(len(contents).to_bytes(8, "little") + contents)
value = digest.hexdigest() + "\n"
if stamp.exists() and stamp.read_text() != value:
    raise SystemExit("LLVM source changed; use a fresh build directory")
stamp.write_text(value)
PY

cmake -S "$build_root/llvm/openmp-18.1.8.src" -B "$build_root/openmp-build" \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DLIBOMP_ENABLE_SHARED=OFF -DLIBOMP_OMPT_SUPPORT=OFF \
    -DOPENMP_ENABLE_LIBOMPTARGET=OFF -DOPENMP_ENABLE_OMPT_TOOLS=OFF
cmake --build "$build_root/openmp-build" --parallel "$CMAKE_BUILD_PARALLEL_LEVEL"
openmp_library="$build_root/openmp-build/runtime/src/libomp.a"
[[ -f "$openmp_library" ]]

sdk_root="$build_root/spfresh"
mkdir -p "$sdk_root/build"
if [[ ! -d "$sdk_root/source" ]]; then
    git init "$sdk_root/source"
    git -C "$sdk_root/source" fetch --depth 1 https://github.com/SPFresh/SPFresh.git \
        5893eb61ee3b18610b6b00f1939be7dae1af8904
    git -C "$sdk_root/source" checkout --detach FETCH_HEAD
fi
cmake -S "$vortex_source/vortex-index-spfresh/native" -B "$sdk_root/build" \
    -DSPFRESH_SOURCE="$sdk_root/source" -DCMAKE_BUILD_TYPE=Release \
    -DZSTD_LIBRARY="$zstd_library" -DZSTD_INCLUDE_DIR="$zstd_include" \
    -DOpenMP_CXX_LIB_NAMES=omp -DOpenMP_omp_LIBRARY="$openmp_library"
bash "$vortex_source/vortex-index-spfresh/native/build.sh" "$sdk_root"
printf '%s\n' "$openmp_library" > "$sdk_root/build/vane-openmp-library.txt"
