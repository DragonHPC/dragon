INSTALL ?= install
INSTALL_DIR ?= ../dist

HEADERS = dragon/bcast.h \
          dragon/channels.h \
          dragon/channelsets.h \
          dragon/global_types.h \
          dragon/managed_memory.h \
          dragon/pmod.h \
          dragon/queue.hpp \
          dragon/return_codes.h \
          dragon/return_codes_map.h \
          dragon/shared_lock.h \
          dragon/utils.h \
          dragon/ddict.h \
          dragon/dictionary.hpp \
          dragon/fli.h

DISTFILES = $(addprefix $(INSTALL_DIR)/include/,$(HEADERS))

OUTPUTS = dragon/return_codes_map.h \
          ../dragon/pydragon_rc.pyx \
          ../dragon/return_codes.pxd


all: $(OUTPUTS)

$(OUTPUTS): dragon/return_codes_map_maker.py dragon/return_codes.h
	cd $(<D) && python3 $(<F)

clean:
	$(RM) -rf $(OUTPUTS) $(DISTFILES) capnp kj

.PHONY: dist
dist: $(DISTFILES)

OS = $(shell uname -s)
$(INSTALL_DIR)/include/%.h: %.h
	@if [ "$(OS)" != "Darwin" ]; then \
		$(INSTALL) -m 644 -D $< $@; \
	else \
        $(INSTALL) -d $(INSTALL_DIR)/include/dragon; \
		$(INSTALL) -m 644 $< $@; \
	fi

$(INSTALL_DIR)/include/%.hpp: %.hpp
	@if [ "$(OS)" != "Darwin" ]; then \
		$(INSTALL) -m 644 -D $< $@; \
	else \
        $(INSTALL) -d $(INSTALL_DIR)/include/dragon; \
		$(INSTALL) -m 644 $< $@; \
	fi
