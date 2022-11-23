#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#


#---------------------build config-------------------------

BASEDIR = /home/ceph

DEBUG_BUILD ?= 1
#EXTRA_CXXFLAGS ?= -I/home/ceph/rocksdb/include 
#EXTRA_CXXFLAGS ?= -I/home/ceph/rocksdb/include -I/home/ceph/bluefs_bench/ceph/src/ -I/home/ceph/bluefs_bench/ceph/build/include -I/home/ceph/bluefs_bench/ceph/build/boost/include
#EXTRA_CXXFLAGS ?= -I/home/ceph/remixdb/include
EXTRA_CXXFLAGS ?= -I/home/ceph/pebblesdb/src/pebblesdb/include
#EXTRA_LDFLAGS ?= -L/home/ceph/rocksdb -ldl -lz -lsnappy -lbz2 -llz4
#EXTRA_LDFLAGS ?= -L/home/ceph/rocksdb -L/usr/local/lib -L/home/ceph/bluefs_bench/ceph/build/lib/ -L/home/ceph/bluefs_bench/ceph/build/boost/lib/ -L/usr/lib64/ -L/home/ceph/bluefs_bench/ceph/build/src/os/CMakeFiles/os.dir/bluestore -ldl -lz -lsnappy -lbz2 -llz4
#EXTRA_LDFLAGS ?= -L/lib #-L/home/ceph/remixdb -ldl -lz -lbz2 -llz4
#EXTRA_LDFLAGS ?= -L/lib -L/usr/lib64 -ldl -lz -lbz2 -llz4  
#EXTRA_LDFLAGS ?= -L/home/ceph/remixdb -ldl -lz -lbz2 -llz4  
EXTRA_LDFLAGS ?= -L/home/seungho/pebblesdb -ldl -lz -lbz2 -llz4

BIND_LEVELDB ?= 0
BIND_ROCKSDB ?= 0
BIND_LMDB ?= 0
BIND_REMIXDB ?= 0
BIND_PEBBLESDB ?= 0
BIND_LCFDB ?= 0

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2
	CPPFLAGS += -DNDEBUG
endif

ifeq ($(BIND_LEVELDB), 1)
	LDFLAGS += -lleveldb
	SOURCES += $(wildcard leveldb/*.cc)
endif

ifeq ($(BIND_ROCKSDB), 1)
	LDFLAGS += -lrocksdb
	SOURCES += $(wildcard rocksdb/*.cc)
	EXTRA_CXXFLAGS += -I${BASEDIR}/rocksdb/include
endif

ifeq ($(BIND_LMDB), 1)
	LDFLAGS += -llmdb
	SOURCES += $(wildcard lmdb/*.cc)
endif

ifeq ($(BIND_REMIXDB), 1)
	LDFLAGS += -lremixdb
	SOURCES += $(wildcard remixdb/*.cc)
	EXTRA_CXXFLAGS += -I${BASEDIR}/remixdb/include
endif

ifeq ($(BIND_PEBBLESDB), 1)
	LDFLAGS += -lpebblesdb
	SOURCES += $(wildcard pebblesdb/*.cc)
	EXTRA_CXXFLAGS += -I${BASEDIR}/pebblesdb/src/include
endif
	
ifeq ($(BIND_LCFDB), 1)
	LDFLAGS += -lrocksdb
	SOURCES += $(wildcard lcfdb/*.cc)
	EXTRA_CXXFLAGS += -I${BASEDIR}/rocksdb/include
endif

#CXXFLAGS += -std=c++11 -Wall -pthread $(EXTRA_CXXFLAGS) -I./
CXXFLAGS += -std=c++17 -Wall -pthread $(EXTRA_CXXFLAGS) -I./
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc)
OBJECTS += $(SOURCES:.cc=.o)
DEPS += $(SOURCES:.cc=.d)
EXEC = ycsbpebbles


all: $(EXEC)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

.cc.o:
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.d : %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cc=.o)' -o $@ $<

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete
	$(RM) $(EXEC)

histogram_test:  test/test_histogram.cc core/histogram.cc
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@


.PHONY: clean
