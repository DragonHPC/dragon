# Makefile for HSTA transport agent
#
ifneq ("$(wildcard $(DRAGON_BASE_DIR)/.dragon-config.mk)","")
    include $(DRAGON_BASE_DIR)/.dragon-config.mk
endif

# TODO: need better way to include files for external dependencies
INCLUDE = -I ../../../include -I ../../../lib ${CONFIG_INCLUDE}
INSTALL = install
INSTALL_DIR = $(DRAGON_BASE_DIR)/bin
INSTALL_LIB_DIR = $(DRAGON_BASE_DIR)/lib
INSTALL_MODE ?= 755

EXE_HSTA = dragon-hsta

# don't build HSTA if there aren't any backends configured
ifneq ("x$(CONFIG_BACKEND_LIB_DEPS)", "x")

SANITIZE=-fsanitize=leak -fsanitize=undefined -fsanitize-address-use-after-scope -fsanitize=address
STACK=-fstack-check
VTABLE=-fvtable-verify=preinit
DEBUG=${SANITIZE} # ${STACK} ${VTABLE}

CXX = g++
LINKER = g++
TARGET_SPECIFIC_CPP_SOURCES = hsta.cpp

ifeq ($(MAKECMDGOALS), test)
	TARGET_SPECIFIC_CXXFLAGS = -O0 -g # ${DEBUG}
	TARGET_SPECIFIC_LDFLAGS = -rdynamic # ${DEBUG} -llsan -lasan
else
	TARGET_SPECIFIC_CXXFLAGS = -O2
	TARGET_SPECIFIC_LDFLAGS = -rdynamic
endif

CXXFLAGS ?= -std=c++17 -fno-exceptions -Wall ${TARGET_SPECIFIC_CXXFLAGS} # -pedantic
LDFLAGS = -lstdc++ -ldl -L ../../../lib -ldragon -lpthread ${TARGET_SPECIFIC_LDFLAGS}

CPP_SOURCES_NO_MAIN = agent.cpp    \
					  channel.cpp  \
					  cq.cpp       \
					  data.cpp     \
					  eager.cpp    \
					  gateway.cpp  \
					  header.cpp   \
					  network.cpp  \
					  request.cpp  \
					  tx_queue.cpp \
					  utils.cpp

CPP_SOURCES = ${CPP_SOURCES_NO_MAIN} ${TARGET_SPECIFIC_CPP_SOURCES}

CPP_OBJECTS = $(CPP_SOURCES:.cpp=.cpp.o)

default: build

includes=$(wildcard *.hpp ../../../lib/&.h ../../../include/dragon/*.h)

%.cpp.o: %.cpp ${includes} Makefile
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c $< -o $@

libdfabric_ofi.so: dragon_fabric.cpp dragon_fabric.hpp Makefile
	$(CXX) -DDF_BACKEND_IS_OFI -O2 -fPIC -c dragon_fabric.cpp -o dragon_fabric.pic.o $(INCLUDE)
	$(CXX) -shared -o ./dfabric_lib/libdfabric_ofi.so dragon_fabric.pic.o $(CONFIG_OFI_LIBS)

libdfabric_ucx.so: dragon_fabric.cpp dragon_fabric.hpp Makefile
	$(CXX) -DDF_BACKEND_IS_UCX -O2 -fPIC -c dragon_fabric.cpp -o dragon_fabric.pic.o $(INCLUDE)
	$(CXX) -shared -o ./dfabric_lib/libdfabric_ucx.so dragon_fabric.pic.o $(CONFIG_UCX_LIBS)

includes:
	cd ../include/dragon && $(MAKE) build

build: $(CPP_OBJECTS) $(CONFIG_BACKEND_LIB_DEPS)
	$(LINKER) -o $(EXE_HSTA) $(CPP_OBJECTS) $(LDFLAGS)
	$(INSTALL) -m $(INSTALL_MODE) -D $(EXE_HSTA) $(INSTALL_DIR)/$(EXE_HSTA)
	$(INSTALL) -m $(INSTALL_MODE) -D $(wildcard ./dfabric_lib/*.so) $(INSTALL_LIB_DIR)
	rm -f $(INSTALL_LIB_DIR)/dummy.so

test: $(CPP_OBJECTS) $(CONFIG_BACKEND_LIB_DEPS)
	$(LINKER) -o $(EXE_HSTA) $(CPP_OBJECTS) $(LDFLAGS)
	$(INSTALL) -m $(INSTALL_MODE) -D $(EXE_HSTA) $(INSTALL_DIR)/$(EXE_HSTA)
	$(INSTALL) -m $(INSTALL_MODE) -D $(wildcard ./dfabric_lib/*.so) $(INSTALL_LIB_DIR)
	rm -f $(INSTALL_LIB_DIR)/dummy.so

else

build: clean

endif # CONFIG_BACKEND_LIB_DEPS

clean:
	rm -f ./*.o ./dfabric_lib/libdfabric_*.so ./*.x  ./$(EXE_HSTA) $(INSTALL_DIR)/$(EXE_HSTA) $(INSTALL_LIB_DIR)/libdfabric_*.so
