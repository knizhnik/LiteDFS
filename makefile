CC=g++
CCFLAGS=-g -Iinc -O0 -fPIC -Wall -Wno-write-strings -Wno-invalid-offsetof -pthread -DDEBUG=1 $(COM_CCFLAGS)
LD = $(CC)
LDFLAGS = -g -pthread 
AR = ar
ARFLAGS = -cru
RANLIB = ranlib


DFS_LIBRARY = lib/liblitedfs.a
DFS_SERVER = bin/dfs_server

DFS_INCLUDES = inc/util.h inc/debug.h inc/event.h inc/mutex.h inc/scoketio.h inc/system.h inc/threadpool.h inc/dfs_client.h inc/dfs_server.h inc/unisock.h inc/types.h
DFS_LIB_SOURCES = src/util.cpp src/debug.cpp src/dfs_client.cpp src/system.cpp src/threadpool.cpp src/unisock.cpp
DFS_SERVER_SOURCES = src/dfs_server.cpp

DFS_LIB_OBJS = $(DFS_LIB_SOURCES:.cpp=.o)
DFS_SERVER_OBJS = $(DFS_SERVER_SOURCES:.cpp=.o)

all: $(DFS_SERVER) $(DFS_LIBRARY) 

.cpp.o : $(DFS_INCLUDES)
	$(CC) $(CCFLAGS) -o $@ -c $< 

$(DFS_LIBRARY): $(DFS_LIB_OBJS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $(DFS_LIB_OBJS)
	$(RANLIB) $@

$(DFS_SERVER): $(DFS_SERVER_OBJS) $(DFS_LIBRARY)
	$(LD) $(LDFLAGS) -o $@ $< $(DFS_LIBRARY) $(STDLIBS)

clean: 
	-rm $(DFS_LIB_OBJS) $(DFS_SERVER_OBJS) $(DFS_LIBRARY) $(DFS_SERVER)

tgz: clean
	cd ..; tar cvzf LiteDFS.tgz LiteDFS

doxygen:
	doxygen doxygen.cfg
