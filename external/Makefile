CC = gcc
CXX = g++
export CXXFLAGS = -fPIC
export CPPFLAGS = -fPIC

ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

ifeq ($(VIRTUAL_ENV),)
	VIRTUAL_ENV = $(CONDA_PREFIX)
endif
CAPNP_INSTALL_DIR:=$(shell echo ${VIRTUAL_ENV})

# Don't add to main build target. This is targeted directly in src/Makefile.
build-capnproto:
ifeq ($(wildcard capnproto/c++/capnp), )
	rm -rf capnproto || true
	git clone https://github.com/capnproto/capnproto.git
	cd capnproto && git checkout b34ec28   # Stick to a defined commit in master, v1.1.0
	cd capnproto/c++ && autoreconf -i && ./configure --prefix=$(CAPNP_INSTALL_DIR) --disable-shared
	cd capnproto/c++ && make && make install
	ln -sf $(CAPNP_INSTALL_DIR)/include/* ../src/include/
endif

clean-capnproto:
	rm -rf capnproto

.PHONY: build
build: build-capnproto

.PHONY: dist
dist: build
	@echo "No external dependencies to distribute"

.PHONY: clean
clean: clean-capnproto

