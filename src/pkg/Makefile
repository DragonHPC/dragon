INSTALL ?= install
INSTALL_DIR ?= ../dist/
INSTALL_MODE ?= 755

PRODUCTS = INSTALL.md \
		   LICENSE.md \
		   README.md \
		   RELEASE_NOTES.md

build:
	@echo "Nothing to do for now"

dist:
	$(foreach p,$(PRODUCTS),$(INSTALL) -m $(INSTALL_MODE) -D $(p) $(INSTALL_DIR)/$(p);)

clean:
	@echo "Nothing to do for now"
