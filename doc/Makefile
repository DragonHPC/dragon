# Minimal makefile for Sphinx documentation
# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?= -T -j 2
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

sph_user = $(shell find `python -m site --user-base` -name sphinx-build || echo "")
sph_def  = $(shell find `python -c "import sysconfig; print(sysconfig.get_path('scripts'))"` -name sphinx-build || echo "")
ifneq ($(wildcard $(sph_user)), )
	SPHINXBUILD = $(sph_user)
else
	ifneq ($(wildcard $(sph_def)), )
		SPHINXBUILD = $(sph_def)
	endif
endif

# git information for tagging the resulting build in artifactory
GITHASH = $(shell git rev-parse --short HEAD)
GITBRANCH = $(shell git branch --show)

# srms diagram files
SRMS_FILES      = $(shell find . -type f -name '*.srms')
SRMS_PNG_FILES  = $(patsubst %.srms, %.srms1.png, $(SRMS_FILES))

# plantuml diagram files
PUML_FILES	= $(shell find . -type f -name '*.puml')
PUML_SVG_FILES	= $(patsubst %.puml, %.svg, $(PUML_FILES))

# We have 3 ways to deal with the plantuml dependency: Download, ship or precompile.
# This is the first.
PLANTUML_URL = https://github.com/plantuml/plantuml/releases/download/v1.2022.1/plantuml-1.2022.1.jar
PLANTUML_JAR = plantuml.jar

# SPHINX_FORTRAN_URL = https://github.com/VACUMM/sphinx-fortran.git
# SPHINX_FORTRAN_FILE = ../external/sphinx-fortran/sphinxfortran/fortran_domain.py

default: build

build:
	@echo "sphinx-build is: $(SPHINXBUILD)"
	cd ../src && doxygen Doxyfile && cd ../doc
	make html

# Make a release package for the artifactory
dist: build
	tar -czvf dragondocs-$(DRAGON_VERSION)-$(GITBRANCH)-$(GITHASH).tar.gz _build/*
	@echo "Dragon docs in: dragondocs-$(DRAGON_VERSION)-$(GITBRANCH)-$(GITHASH).tar.gz"


# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

clean: Makefile
	@echo $(SRMS_FILES)
	rm -Rf _build $(PUML_SVG_FILES) $(SRMS_PNG_FILES) $(PLANTUML_JAR) $(patsubst %.srms, %.srms2.png, $(SRMS_FILES))
	rm -rf ../external/sphinx-fortran

# If the JAR file isn't already present, download it
$(PLANTUML_JAR):
	curl -sSfL  $(PLANTUML_URL) -o $(PLANTUML_JAR)

# If the sphinx fortran domain isn't present, download it and install it.
$(SPHINX_FORTRAN_FILE):
	cd ../external && git clone $(SPHINX_FORTRAN_URL) && cd sphinx-fortran && python setup.py install

puml_diagrams: $(PUML_SVG_FILES)
srms_diagrams: $(SRMS_PNG_FILES)

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).

html: $(SPHINX_FORTRAN_FILE) puml_diagrams srms_diagrams Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

release: html
	rm -Rf /cray/css/dragon/dragondocs/html
	cp -Rf $(BUILDDIR)/html /cray/css/dragon/dragondocs

%.svg : %.puml $(PLANTUML_JAR)
	java -jar $(PLANTUML_JAR) -tsvg $<

%.srms1.png : %.srms
	cd $(<D) && srms2png $(<F)
