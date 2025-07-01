# plantuml diagram files
PUML_FILES	= $(shell find . -type f -name '*.puml')
PUML_SVG_FILES	= $(patsubst %.puml, %.svg, $(PUML_FILES))

# We have 3 ways to deal with the plantuml dependency: Download, ship or precompile.
# This is the first.
PLANTUML_URL = https://github.com/plantuml/plantuml/releases/download/v1.2022.1/plantuml-1.2022.1.jar
PLANTUML_JAR = plantuml.jar

default: puml_diagrams

# If the JAR file isn't already present, download it
$(PLANTUML_JAR):
	curl -sSfL  $(PLANTUML_URL) -o $(PLANTUML_JAR)

clean: Makefile
	rm -Rf _build $(PUML_SVG_FILES) $(PLANTUML_JAR)

puml_diagrams: $(PUML_SVG_FILES)

%.svg : %.puml $(PLANTUML_JAR)
	java -jar $(PLANTUML_JAR) -tsvg $<
