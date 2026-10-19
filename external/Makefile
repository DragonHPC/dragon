CC = gcc
CXX = g++
export CXXFLAGS = -fPIC
export CPPFLAGS = -fPIC

CAPNP_INSTALL_DIR ?= $(shell pwd)/../src

CAPNP_TARGETS = $(CAPNP_INSTALL_DIR)/bin/capnp \
			    $(CAPNP_INSTALL_DIR)/include/capnp/message.h

capnproto:
	@if curl -fsSL --connect-timeout 10 --max-time 30 -I https://arti.hpc.amslabs.hpecorp.net:443/artifactory/github.com-remote/capnproto/capnproto/archive/refs/tags/v1.5.0.tar.gz >/dev/null 2>&1; then \
		curl -fsSL --connect-timeout 10 --max-time 30 -o v1.5.0.tar.gz https://arti.hpc.amslabs.hpecorp.net:443/artifactory/github.com-remote/capnproto/capnproto/archive/refs/tags/v1.5.0.tar.gz; \
	else \
		curl -fsSL --connect-timeout 10 --max-time 30 -o v1.5.0.tar.gz https://github.com/capnproto/capnproto/archive/refs/tags/v1.5.0.tar.gz; \
	fi; \
	mkdir -p capnproto && tar -xzf v1.5.0.tar.gz --strip-components=1 -C capnproto

capnproto/c++/Makefile: capnproto
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

