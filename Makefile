PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ducklake
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Core extensions that we need for crucial testing
DEFAULT_TEST_EXTENSION_DEPS=
# For cloud testing we also need these extensions
FULL_TEST_EXTENSION_DEPS=httpfs

# Aws and Azure have vcpkg dependencies and therefore need vcpkg merging
ifeq (${BUILD_EXTENSION_TEST_DEPS}, full)
	USE_MERGED_VCPKG_MANIFEST:=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Keep Vane-only variables outside DuckDB's native extension build.
VANE_EXTENSION_MAKEFILE := $(PROJ_DIR)vane-extension-ci-tools/makefiles/vane_extension.Makefile
VANE_EXTENSION_TARGETS := vane_verify_ci_tools vane_validate vane_prepare vane_identity \
	vane_native vane_ci vane_wheel_dependencies vane_wheel
.PHONY: $(VANE_EXTENSION_TARGETS)

$(VANE_EXTENSION_TARGETS):
	@test -f "$(VANE_EXTENSION_MAKEFILE)" || { \
		printf 'initialize vane-extension-ci-tools before running %s\n' "$@" >&2; \
		exit 2; \
	}
	+$(MAKE) --no-print-directory -f "$(VANE_EXTENSION_MAKEFILE)" "$@" \
		VANE_EXTENSION_ROOT="$(abspath $(PROJ_DIR))"
