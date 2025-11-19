ifneq ("$(wildcard $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk)","")
    include $(DRAGON_BASE_DIR)/dragon/.dragon-config.mk
endif

export INSTALL = install
export PYTHON = $(shell which python3 | awk '{if ($$1 == "") { print "python" } else { print "python3"}}')
export PIP = $(shell which pip3 | awk '{if ($$1 == "") { print "pip" } else { print "pip3"}}')

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

LIBRARIES := lib/libdragon.so lib/libpmod.so $(wildcard lib/libdfabric_*.so)

TOOLS := tools/dragon-cleanup \
         tools/dragon-node-cleanup

DRAGON_RUN_TOOLS := tools/dragon_cleanup.py \
                    tools/dragon_run \
                    tools/__init__.py


BINARIES := $(addprefix bin/, $(notdir $(TOOLS)))

export SYMBOLIC_BIN := $(abspath dragon/bin)
SYMBOLIC_LIB := $(abspath dragon/lib)

EXTERNAL := ../external/capnproto

CY_FILES := dragon/pydragon_rc.pyx \
            dragon/return_codes.pxd


.PHONY: all
all: build


.PHONY: build
build: $(EXTERNAL) $(LIBRARIES) $(CYFILES) $(BINARIES) install-pythondeps
	# Build cython code
	$(PYTHON) setup.py build $(SETUP_PY_BUILD_FLAGS)
	# Build cli entry points
	$(PYTHON) setup.py develop
	# Symbolic link to lib. Needs to go here and only used for dev builds. The setup.py step clears it otherwise
	ln -sf $(abspath lib) $(SYMBOLIC_LIB)


.PHONY: $(EXTERNAL)
$(EXTERNAL):
	$(MAKE) -C ../external build-capnproto

#lib:
.PHONY: lib
lib:
	$(MAKE) -C lib

.PHONY: include
include:
	$(MAKE) -C include

lib/libdragon.so: $(EXTERNAL) include lib

# Copy tools dir to bin
bin/$(notdir $(TOOLS)): $(TOOLS) $(SYMBOLIC_BIN)
	cp -r $(TOOLS) bin/

# Create a symbolic link into src/dragon/bin of src/bin to keep paths written into
# code actually exits. Do a -f because mtimes will do weird stuff with make
# and symbolic links
$(SYMBOLIC_BIN):
	mkdir -p $(abspath bin)
	ln -sf $(abspath bin) $@

$(CY_FILES):
	$(MAKE) -C include ../$@

.PHONY: clean
clean:
	$(PYTHON) setup.py clean --all
	$(RM) -r build *.egg-info
	$(RM) $(LIBRARIES) $(BINARIES) $(CY_FILES)
	$(MAKE) -C lib clean
	$(MAKE) -C include clean

# create a package for release
.PHONY: dist
dist: build
	$(MAKE) -C include dist
	$(PYTHON) setup.py clean --all
	$(PYTHON) setup.py build $(SETUP_PY_BUILD_FLAGS)
# Prepare to files to be included in the distribution.
# This code copies the message_defs.capnp file into the wheel file
# which is needed for the captain proto support.
	rm -rf build/dragon_wheel
	mkdir -p build/dragon_wheel/dragon/infrastructure
	cp -f lib/message_defs.capnp build/dragon_wheel/dragon/infrastructure/
	mkdir -p build/dragon_wheel/dragon/lib
	cp -f lib/libdragon.so lib/libpmod.so build/dragon_wheel/dragon/lib
	mkdir -p build/dragon_wheel/dragon/tools
	cp -rf $(DRAGON_RUN_TOOLS) build/dragon_wheel/dragon/tools
	cp -f lib/libdfabric_*.so build/dragon_wheel/dragon/lib || true
	mkdir -p build/dragon_wheel/dragon/bin
	cp -f $(BINARIES) build/dragon_wheel/dragon/bin
	mkdir -p build/dragon_wheel/dragon/include
	cp -rf include/dragon build/dragon_wheel/dragon/include
	cp -Lrf include/capnp build/dragon_wheel/dragon/include
	cp -Lrf include/kj build/dragon_wheel/dragon/include
	$(PYTHON) setup.py bdist_wheel --skip-build --bdist-dir=$(abspath build/dragon_wheel)
	$(MAKE) -C pkg dist
	$(MAKE) -C ../test dist
	$(MAKE) release

.PHONY: release
release:
	test -d dist || $(MAKE) dist
	mkdir -p release/dragon
	cp -pr dist/* release/dragon
	cp -pr ../examples release/dragon
	rm -rd release/dragon/examples/dragon_native/mpi
	rm -rf release/dragon/examples/multiprocessing/user_adapter
	rm -rf release/dragon/examples/dragon_core
	rm -rf release/dragon/examples/smartsim
	rm -rf release/dragon/include
	rm -f release/dragon/examples/multiprocessing/lock_performance.py
	rm -f release/dragon/examples/multiprocessing/scipy_image_demo.py
	mkdir -p release/dragon/examples/multiprocessing/multi-node_tests
	cp ../test/multi-node/test_process.py release/dragon/examples/multiprocessing/multi-node_tests
	cp ../test/multi-node/test_pool.py release/dragon/examples/multiprocessing/multi-node_tests
	cp ../test/multi-node/test_queue.py release/dragon/examples/multiprocessing/multi-node_tests
	cp ../test/multi-node/test_connection.py release/dragon/examples/multiprocessing/multi-node_tests
	mkdir -p release/dragon/examples/dragon_native/multi-node_tests
	cp ../test/multi-node/test_lock.py  release/dragon/examples/dragon_native/multi-node_tests
	cp ../test/multi-node/test_machine.py  release/dragon/examples/dragon_native/multi-node_tests
	cp ../test/multi-node/test_barrier.py  release/dragon/examples/dragon_native/multi-node_tests
	cp ../src/dragon/telemetry/imports/*.yaml  release/dragon/examples/dragon_telemetry
	cp ../src/dragon/telemetry/imports/*.json  release/dragon/examples/dragon_telemetry
# Repair the wheel to use manylinux tag before copying it to release and remove the un-tagged version
	auditwheel repair -w $(abspath release/dragon) $(abspath release/dragon)/dragonhpc*-linux_*.whl
	rm -rf $(abspath release/dragon)/dragonhpc*-linux_*whl
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
	$(RM) -r dist || true
	$(RM) $(SYMBOLIC_BIN)


# install Python packages needed to build Dragon if desired
.PHONY: install-pythondeps
install-pythondeps:
	$(PIP) install -r requirements.txt

build-pythondev:
	$(MAKE) -C ../external
	$(MAKE) install-pythondeps
	$(MAKE) build
