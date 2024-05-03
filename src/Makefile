export INSTALL = install
export PYTHON = $(shell which python3 | awk '{if ($$1 == "") { print "python" } else { print "python3"}}')
export PIP = $(shell which pip3 | awk '{if ($$1 == "") { print "pip" } else { print "pip3"}}')

GITHASH = $(shell git rev-parse --short HEAD)

ifneq ($(DRAGON_BUILD_NTHREADS),)
SETUP_PY_BUILD_FLAGS += -j $(DRAGON_BUILD_NTHREADS)
endif

export INSTALL_DIR = $(abspath dist)

LIBRARIES := lib/libdragon.so lib/libpmsgqueue.so lib/libpmod.so

TOOLS := tools/dragon-cleanup \
         tools/dragon-node-cleanup

BINARIES := $(addprefix bin/, $(notdir $(TOOLS)))

EXTERNAL := ../external/capnproto

CY_FILES := dragon/pydragon_rc.pyx dragon/return_codes.pxd

.PHONY: all
all: build

.PHONY: build
build: $(EXTERNAL) $(CYFILES) $(LIBRARIES) $(BINARIES)
	$(PYTHON) setup.py build $(SETUP_PY_BUILD_FLAGS)

.PHONY: $(EXTERNAL)
$(EXTERNAL):
	$(MAKE) -C ../external build-capnproto

.PHONY: $(LIBRARIES)
$(LIBRARIES): %.so:
	$(MAKE) -C $(@D) $(@F)

.PHONY: $(BINARIES)

# Copy tools dir to bin
bin/$(notdir $(TOOLS)): $(TOOLS)
	mkdir -p bin/
	cp -r $(TOOLS) bin/


.PHONY: $(CY_FILES)
$(CY_FILES):
	$(MAKE) -C include ../$@

.PHONY: clean
clean:
	$(PYTHON) setup.py clean --all
	$(RM) -r build *.egg-info
	$(RM) $(LIBRARIES) $(BINARIES) $(CY_FILES)
	$(MAKE) -C dragon/launcher/src clean
	$(MAKE) -C lib clean
	$(MAKE) -C include clean

# create a package for release
.PHONY: dist
dist: $(addprefix $(INSTALL_DIR)/,$(LIBRARIES) $(BINARIES)) $(CY_FILES)
	$(MAKE) -C include dist
	$(MAKE) -C ../external dist
	$(PYTHON) setup.py clean --all
	$(PYTHON) setup.py build $(SETUP_PY_BUILD_FLAGS) --cythonize -f
# Prepare to files to be included in the distribution.
	cp ../external/pycapnp/pycapnp*.whl dist
	cp bin/dragon-install dist
# This code copies the message_defs.capnp file into the wheel file
# which is needed for the captain proto support.
	rm -rf build/dragon_wheel
	mkdir build/dragon_wheel/
	mkdir build/dragon_wheel/dragon
	mkdir build/dragon_wheel/dragon/infrastructure
	cp -f lib/message_defs.capnp build/dragon_wheel/dragon/infrastructure/
# The following lines do not work to get the library files into the wheel file
# because RPATH is not set correctly in them. Code is left here for now because
# it does show how we might copy the files in should we solve the RPATH problem.
#	mkdir build/dragon_wheel/lib
#	cp -f lib/libdragon.so build/dragon_wheel/lib
#	cp -f lib/libpmod.so build/dragon_wheel/lib
#	cp -f lib/libpmsgqueue.so build/dragon_wheel/lib
	$(PYTHON) setup.py bdist_wheel --skip-build --bdist-dir=$(abspath build/dragon_wheel)
	$(MAKE) -C modulefiles dist
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
	rm -rf release/dragon/examples/client_logging
	rm -f release/dragon/examples/multiprocessing/lock_performance.py
	rm -f release/dragon/examples/multiprocessing/scipy_image_demo.py
	mkdir  -p release/dragon/examples/multiprocessing/multi-node_tests
	cp ../test/multi-node/test_process.py release/dragon/examples/multiprocessing/multi-node_tests
	cp ../test/multi-node/test_pool.py release/dragon/examples/multiprocessing/multi-node_tests
	cp ../test/multi-node/test_queue.py release/dragon/examples/multiprocessing/multi-node_tests
	cp ../test/multi-node/test_connection.py release/dragon/examples/multiprocessing/multi-node_tests
	mkdir  -p release/dragon/examples/dragon_native/multi-node_tests
	cp ../test/multi-node/test_lock.py  release/dragon/examples/dragon_native/multi-node_tests
	cp ../test/multi-node/test_machine.py  release/dragon/examples/dragon_native/multi-node_tests
	cp ../test/multi-node/test_barrier.py  release/dragon/examples/dragon_native/multi-node_tests
	mv release/dragon release/dragon-$(DRAGON_VERSION)
	cd release && tar -czvf dragon-$(DRAGON_VERSION)-$(GITHASH).tar.gz dragon-$(DRAGON_VERSION)
	cd release && rm -rf dragon-$(DRAGON_VERSION)
	@echo "Dragon package is: release/dragon-$(DRAGON_VERSION)-$(GITHASH).tar.gz"


$(INSTALL_DIR)/lib/lib%.so: lib/lib%.so
	$(INSTALL) -m 755 -D $< $@

$(INSTALL_DIR)/bin/%: bin/%
	$(INSTALL) -m 755 -D $< $@


.PHONY: distclean
distclean: clean
	$(RM) -r release doxygen
	$(MAKE) -C ../test clean
	$(MAKE) -C pkg clean
	$(MAKE) -C modulefiles clean
	$(RM) -r dist

# install Python packages needed to build Dragon if desired
install-pythondeps:
	$(PIP) install --user -r requirements.txt -c constraints.txt

build-pythondev:
	$(MAKE) -C ../external
	$(MAKE) install-pythondeps
	$(MAKE) build

# The following are steps to take to publish to PyPI as the dragonhpc package.
# These steps were created by following the tutorial at:
# https://packaging.python.org/en/latest/tutorials/packaging-projects/
# We first need to build the package so we have a wheel file to distribute.
# It might be that we want to include other files as well, like the source
# or the examples. In that case a .tar.gz file is required of files to
# distribute. It might be that we include .so files in such these files?
# 1. Create an account on PyPI.org and create an API Token. Set the API
#    token Scope for "Entire account". Copy the API token from there.
#    If you like, you can create a ~/.pypirc file with the following format.
#    If you close the API create window in browser you will not be able to
#    get the API token back, so copy it first. chmod og-rxw on this file so
#    only user has permission.
#    [pypi]
#       username = __token__
#       password = API_token_include_the_pypi-_at_the_beginning
# 2. Build the package (release target). Wheel packages to be uploaded
#    must be gathered. If there are specific requirements for different
#    versions of Python, then multiple wheel files are required. Otherwise
#    you can specifiy that one package is good for multiple versions of Python.
#    It is customary to supply wheels for the current and two previous versions
#    of Python. If there are files to upload, then they should be gathered into a
#    gzipped tar file.
# 3. Execute:
#    python3 -m pip install --upgrade twine
#    This upgrades twine for package upload
# 4. To upload you might want the ~/.pypirc. If not, the user id is
#    __token__ and the password is that API key that you saved earlier.
#    Then you execute the following.
#
#    python3 -m twine upload --repository pypi [whl-files] [gzipped-tar-file]
#
#    Again, the gzipped tar file is optional.
