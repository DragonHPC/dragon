ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
export CXXFLAGS = -fPIC
export CPPFLAGS = -fPIC
#export LDFLAGS = -L$(ROOT_DIR)/../_env/lib
CC = gcc
CXX = g++

configure: configure-cpython
build: build-cpython
clean: clean-cpython clean-capnproto
# Don't add to main build target. This is targeted directly in src/Makefile.
build-capnproto:
ifeq ($(wildcard capnproto/c++/capnp), )
	git clone https://github.com/capnproto/capnproto.git
	cd capnproto && git checkout master
	cd capnproto/c++ && find . -type f -not -path '*/\.*' -exec sed -i 's/KJ_LOG(WARNING,/KJ_LOG(INFO,/g' {} +
	cd capnproto/c++ && autoreconf -i && ./configure --prefix=$(ROOT_DIR)/../_env --disable-shared
	cd capnproto/c++ && make && make install
	cd capnproto/c++ && autoreconf -i && ./configure --prefix=$(ROOT_DIR)/../_env
	git clone https://github.com/capnproto/pycapnp.git
	@echo "capnproto/c++/capnp and pycapnp exist, now building."
	cd capnproto/c++ && make && make install
	cp -f $(ROOT_DIR)/../_env/lib/libcapnp.a $(ROOT_DIR)/../src/lib
	cp -f $(ROOT_DIR)/../_env/lib/libkj.a $(ROOT_DIR)/../src/lib
	ln -sf $(ROOT_DIR)/../_env/include/capnp $(ROOT_DIR)/../src/include/
	ln -sf $(ROOT_DIR)/../_env/include/kj $(ROOT_DIR)/../src/include/
	cd pycapnp && \
	    PKG_CONFIG_PATH=$(ROOT_DIR)/../_env/lib/pkgconfig:$PKG_CONFIG_PATH \
	    PATH=$(ROOT_DIR)/../_env/bin:${PATH} \
	    LDFLAGS=-L$(ROOT_DIR)/../_env/lib CFLAGS=-I$(ROOT_DIR)/../_env/include \
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
