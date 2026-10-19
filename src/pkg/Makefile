INSTALL ?= install
INSTALL_DIR ?= ../dist/
INSTALL_MODE ?= 755

PRODUCTS = INSTALL.md \
		   LICENSE.md \
		   README.md \
		   RELEASE_NOTES.md \
		   CHANGELOG.md


build:
	@echo "Nothing to do for now"

OS = $(shell uname -s)
ifneq ("$(OS)","Darwin")
	INSTALL_FLAG = "-D"
else
	INSTALL_FLAG =
endif

dist:
	@if [ "$(OS)" == "Darwin" ]; then $(INSTALL) -d $(INSTALL_DIR); fi
	$(foreach p,$(PRODUCTS),$(INSTALL) -m $(INSTALL_MODE) $(INSTALL_FLAG) $(p) $(INSTALL_DIR)/$(p);)

clean:
	@echo "Nothing to do for now"
