CC = gcc
CXX = g++
export CXXFLAGS = -fPIC
export CPPFLAGS = -fPIC

configure: configure-cpython
build: build-cpython
clean: clean-cpython clean-capnproto

ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
CAPNP_INSTALL_DIR:=$(shell echo ${VIRTUAL_ENV})

# Don't add to main build target. This is targeted directly in src/Makefile.
build-capnproto:
ifeq ($(wildcard capnproto/c++/capnp), )
	git clone https://github.com/capnproto/capnproto.git
	cd capnproto && git checkout master
	cd capnproto/c++ && find . -type f -not -path '*/\.*' -exec sed -i 's/KJ_LOG(WARNING,/KJ_LOG(INFO,/g' {} +
	cd capnproto/c++ && autoreconf -i && ./configure --prefix=$(CAPNP_INSTALL_DIR) --disable-shared
	cd capnproto/c++ && make && make install
	cd capnproto/c++ && autoreconf -i && ./configure --prefix=$(CAPNP_INSTALL_DIR)
	git clone https://github.com/capnproto/pycapnp.git
	@echo "capnproto/c++/capnp and pycapnp exist, now building."
	cd capnproto/c++ && make && make install
	cp -f $(CAPNP_INSTALL_DIR)/lib/libcapnp.a $(ROOT_DIR)/../src/lib
	cp -f $(CAPNP_INSTALL_DIR)/lib/libkj.a $(ROOT_DIR)/../src/lib
	ln -sf $(CAPNP_INSTALL_DIR)/include/capnp $(ROOT_DIR)/../src/include/
	ln -sf $(CAPNP_INSTALL_DIR)/include/kj $(ROOT_DIR)/../src/include/
	cd pycapnp && \
	    PKG_CONFIG_PATH=$(CAPNP_INSTALL_DIR)/lib/pkgconfig:$PKG_CONFIG_PATH \
	    PATH=$(CAPNP_INSTALL_DIR)/bin:${PATH} \
	    LDFLAGS=-L$(CAPNP_INSTALL_DIR)/lib CFLAGS=-I$(CAPNP_INSTALL_DIR)/include \
	    pip --verbose wheel .
	cd pycapnp && pip install pycapnp*.whl
endif

clean-capnproto:
	rm -rf capnproto
	rm -rf pycapnp

configure-cpython:
ifneq ($(wildcard cpython_master/Makefile), )
	@echo "cpython_master/Makefile exists, not running configure"
else
ifneq ($(DRAGON_CPYTHON_PATH), )
	cd cpython_master && ./configure --prefix=$(DRAGON_CPYTHON_PATH)
else
	@echo "You must run: module load dragon-python-dev, if you want to configure CPython"
endif
endif


build-cpython: configure-cpython
ifneq ($(DRAGON_CPYTHON_PATH), )
	cd cpython_master && make -j && make install
else
	@echo "Skipping building CPython"
	@echo "You must run: module load dragon-python-dev, if you want to build CPython"
endif

clean-cpython:
ifneq ($(DRAGON_CPYTHON_PATH), )
	cd cpython_master && make distclean
	rm -rf $(DRAGON_CPYTHON_PATH)
else
	@echo "Skipping cleaning CPython"
endif

dist: build
	@echo "No external dependencies to distribute"
