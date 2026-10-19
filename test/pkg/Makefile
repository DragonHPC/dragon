export PYTHON = $(shell which python3 | awk '{if ($$1 == "") { print "python" } else { print "python3"}}')

default: accepttest

unittest:
	$(PYTHON) test_utils.py -v -f
	$(PYTHON) test_channels.py -v -f

accepttest:
	cd multi-node && make all DEF_NODES="-N 2"
