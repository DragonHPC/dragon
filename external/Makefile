CC = gcc
CXX = g++
export CXXFLAGS = -fPIC
export CPPFLAGS = -fPIC

CAPNP_INSTALL_DIR ?= $(shell pwd)/../src

CAPNP_TARGETS = $(CAPNP_INSTALL_DIR)/bin/capnp \
			    $(CAPNP_INSTALL_DIR)/include/capnp/message.h

capnproto:
	git clone https://github.com/capnproto/capnproto.git

capnproto/c++/Makefile: capnproto
	cd capnproto && git checkout b34ec28   # Stick to a defined commit in master, v1.1.0
	cd capnproto/c++ && autoreconf -i && ./configure --prefix=$(CAPNP_INSTALL_DIR) --disable-shared

$(CAPNP_TARGETS): capnproto/c++/Makefile
	cd capnproto/c++ && $(MAKE) && $(MAKE) install

build-capnproto: $(CAPNP_TARGETS)

clean-capnproto:
	cd capnproto/c++ && $(MAKE) clean

.PHONY: build
build: build-capnproto

.PHONY: dist
dist: build
	@echo "No external dependencies to distribute"

.PHONY: clean
clean: clean-capnproto

