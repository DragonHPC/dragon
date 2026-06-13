ENVIRONMENT ?= dev

ifneq ("$(wildcard $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk)","")
    include $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk
endif

OS := $(shell uname)

# These don't hurt to set here.
# For older Mac OS replace with MACOSX_DEPLOYMENT_TARGET=10.13 and MACOSX_DEPLOYMENT_NAME=10_13
ifeq "$(OS)" "Darwin"
	export MACOSX_DEPLOYMENT_TARGET=26.0
	export MACOSX_DEPLOYMENT_NAME=26_0
	export _PYTHON_HOST_PLATFORM=macosx-11.0-arm64
	export ARCHFLAGS=-arch arm64
endif

export INSTALL = install
export PYTHON = $(shell which python3 | awk '{if ($$1 == "") { print "python" } else { print "python3"}}')
# Check if UV_AVAILABLE is set to 1, if so use uv pip, otherwise use pip3/pip
ifeq ($(UV_AVAILABLE),1)
	export PIP = uv pip install --index-strategy unsafe-best-match
else
	export PIP = $(shell which pip3 | awk '{if ($$1 == "") { print "pip install" } else { print "pip3 install"}}')
endif

MIN_GCC_VERSION=9
MIN_PYTHON_MAJOR_VERSION=3
MIN_PYTHON_MINOR_VERSION=10

# Do sanity checks to make sure gcc and python are recent enough
GCC_VERSION := $(shell gcc -dumpversion | cut -f 1 -d'.')
GCC_VERSION_OK := $(shell $(PYTHON) -c 'print(int($(GCC_VERSION) >= $(MIN_GCC_VERSION)))')
ifneq "$(GCC_VERSION_OK)" "1"
    $(warning ================================)
    $(warning |                              |)
    $(warning |    GCC VERSION IS TOO OLD    |)
    $(warning |       USE GCC >= 9           |)
    $(warning |                              |)
    $(warning ================================)
    # Do something stupid to cause an exit
    exit
endif

PYTHON_MAJOR_VERSION=$(shell python3 -c 'import sys; print(f"{sys.version_info.major}")')
PYTHON_MINOR_VERSION=$(shell python3 -c 'import sys; print(f"{sys.version_info.minor}")')
PYTHON_VERSION_OK := $(shell $(PYTHON) -c 'print(int($(PYTHON_MAJOR_VERSION) >= $(MIN_PYTHON_MAJOR_VERSION) and $(PYTHON_MINOR_VERSION) >= $(MIN_PYTHON_MINOR_VERSION)))')
ifneq "$(PYTHON_VERSION_OK)" "1"
    $(warning ================================)
    $(warning |                              |)
    $(warning |   PYTHON VERSION IS TOO OLD  |)
    $(warning |      USE PYTHON >= $(MIN_PYTHON_MAJOR_VERSION).$(MIN_PYTHON_MINOR_VERSION)       |)
    $(warning |                              |)
    $(warning ================================)
    # Do something stupid to cause an exit
    exit
endif

# Set up build
GITHASH = $(shell git rev-parse --short HEAD)

ifneq ($(DRAGON_BUILD_NTHREADS),)
SETUP_PY_BUILD_FLAGS += -j $(DRAGON_BUILD_NTHREADS)
endif

export INSTALL_DIR = $(abspath dist)

LIBRARIES := lib/libdragon.so lib/libpmod.so

TOOLS := tools/dragon-cleanup \
         tools/dragon-node-cleanup

DRAGON_RUN_TOOLS := tools/dragon_cleanup.py \
                    tools/dragon_run \
                    tools/__init__.py


BINARIES := $(addprefix bin/, $(notdir $(TOOLS)))

SYMBOLIC_BIN := $(abspath dragon/bin)

EXTERNAL_TARGET := include/capnp/message.h
EXTERNAL_DIR := ../external/capnproto

CY_FILES := dragon/pydragon_rc.pyx \
            dragon/return_codes.pxd


# These are all compiled via python and Cython. We track them
# here so that we can clean them up as python will not do that for us.
CY_SRC = dragon/pydragon_channels.pyx        \
         dragon/pydragon_dpmix.pyx           \
         dragon/pydragon_dtypes.pyx          \
         dragon/pydragon_fli.pyx             \
         dragon/pydragon_heap.pyx            \
         dragon/pydragon_lock.pyx            \
         dragon/pydragon_managed_memory.pyx  \
         dragon/pydragon_perf.pyx            \
         dragon/pydragon_pmod.pyx            \
         dragon/pydragon_rc.pyx              \
         dragon/pydragon_utils.pyx           \

CY_INC = dragon/channels.pxd        \
         dragon/dtypes_inc.pxd      \
         dragon/managed_memory.pxd

CY_BUILD_FILES := $(CY_SRC:.pyx=.c) $(CY_INC:.pxd=.h)

.PHONY: all
all: build

OS = $(shell uname -s)


.PHONY: build
build: $(EXTERNAL_TARGET) $(LIBRARIES) $(CYFILES) dragon-popen $(BINARIES)


.PHONY: $(EXTERNAL_DIR)
$(EXTERNAL_DIR):
	$(MAKE) -C ../external build

$(EXTERNAL_TARGET): $(EXTERNAL_DIR)

#lib:
.PHONY: lib
lib:
	$(MAKE) -C lib

.PHONY: dragon-popen
dragon-popen: lib/libdragon.so
	$(MAKE) -C dragon/localservices

.PHONY: include
include:
	$(MAKE) -C include

lib/libdragon.so: $(EXTERNAL_TARGET) include lib

lib/libpmod.so: lib/libdragon.so

# Copy tools dir to bin
bin/$(notdir $(TOOLS)): $(TOOLS) $(SYMBOLIC_BIN)
	cp -r $(TOOLS) bin/

# Create a symbolic link into src/dragon/bin of src/bin to keep paths written into
# code actually exits. Do a -f because mtimes will do weird stuff with make
# and symbolic links
$(SYMBOLIC_BIN):
	mkdir -p $(abspath bin)
	ln -sf $(abspath bin) $@

$(BINARIES): lib/libdragon.so

$(CY_FILES):
	$(MAKE) -C include ../$@

.PHONY: clean
clean:
	pyclean . --debris all
	$(RM) -r build *.egg-info
	$(RM) $(LIBRARIES) $(BINARIES) $(CY_FILES)
	$(MAKE) -C dragon/localservices clean
	$(MAKE) -C lib clean
	$(MAKE) -C include clean

# create a package for release
.PHONY: dist
dist: build
	$(MAKE) -C pkg dist
	$(MAKE) -C ../test dist
	$(MAKE) release

.PHONY: release
release:
	mkdir -p release/dragon
# If Linux then repair the wheel to use manylinux tag before copying it to release and remove the un-tagged version
# If MacOS then use delocate-wheel to put it in the right state. Also rename wheel file to something sensible.
	if [ "$(OS)" != "Darwin" ]; then \
		patchelf --version; \
		auditwheel repair -w $(abspath release/dragon) $(abspath release/dragon)/dragonhpc*-linux_*.whl; \
		rm -rf $(abspath release/dragon)/dragonhpc*-linux_*whl; \
	else \
		DYLD_LIBRARY_PATH=lib delocate-wheel -w $(abspath release/dragon) $(abspath release/dragon)/dragonhpc*-macosx_*.whl; \
        mv $(abspath release/dragon)/dragonhpc*-macosx_*.whl $(abspath release/dragon)/dragonhpc-$(DRAGON_VERSION)-cp$(PYTHON_MAJOR_VERSION)$(PYTHON_MINOR_VERSION)-cp$(PYTHON_MAJOR_VERSION)$(PYTHON_MINOR_VERSION)-macosx_$(MACOSX_DEPLOYMENT_NAME)_arm64.whl; \
	fi
	mv release/dragon release/dragon-$(DRAGON_VERSION)
	cd release && tar -czvf dragon-$(DRAGON_VERSION)-$(GITHASH).tar.gz dragon-$(DRAGON_VERSION)
	cd release && rm -rf dragon-$(DRAGON_VERSION)
	@echo "Dragon package is: release/dragon-$(DRAGON_VERSION)-$(GITHASH).tar.gz"



.PHONY: distclean
distclean: clean
	$(RM) -r release doxygen || true
	$(MAKE) -C ../test clean || true
	$(MAKE) -C ../external clean || true
	$(MAKE) -C pkg clean || true
	$(MAKE) -C modulefiles clean || true
	$(MAKE) -C include clean || true
	$(RM) $(CY_BUILD_FILES) || true
	$(RM) dragon/*.so || true
	$(RM) -r dist || true
	$(RM) $(SYMBOLIC_BIN) || true
	$(RM) dragon/lib