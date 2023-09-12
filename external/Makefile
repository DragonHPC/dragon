CC = gcc
CXX = g++

build: build-cpython
clean: clean-cpython

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
