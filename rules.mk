BIN_DIR       = ../../../bin
LIB_DIR       = ../../../lib
LOCAL_DIR     = /usr/local
GCC_BIN_DIR   = $(NEW_GNU_GCC)/bin
GCC_LIB_DIR   = $(NEW_GNU_GCC)/lib
GCC_LIB64_DIR = $(NEW_GNU_GCC)/lib64

################################################################
APP_INCL  = -I../include
BASE_INCL = -I../../base/include
XDBO_INCL = -I$(RA_ROOT)/include
JNI_INCL  = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
HDFS_INCL = -I$(HADOOP_HOME)/include
TASK_INCL = -I$(RACTL_HOME)/inc
AB2_INCL  = $(shell ab2-config --cflags)

################################################################
BASE_LIB = -L$(LIB_DIR) -lbase
XDBO_LIB = -L$(RA_ROOT)/lib -lxdbo2 -lzookeeper_mt
JNI_LIB  = -L$(JAVA_HOME)/jre/lib/amd64/server -ljvm
HDFS_LIB = -L$(HADOOP_HOME)/lib/native -lhdfs
TASK_LIB = -L$(RACTL_HOME)/lib -lrastate
AB2_LIBS = $(shell ab2-config --libs64)

################################################################
CPP = $(GCC_BIN_DIR)/g++
CPP_FLAG = -g -m64 -Wall -O2 -DLINUX

################################################################
AR = ar
AR_FLAG = rv

################################################################
CP = cp
MV = mv
RM = rm -f

################################################################
INCLS = $(APP_INCL) $(BASE_INCL) $(XDBO_INCL) $(JNI_INCL) $(HDFS_INCL) $(TASK_INCL) $(AB2_INCL)
LIBS  = $(BASE_LIB) $(XDBO_LIB) $(JNI_LIB) $(HDFS_LIB) $(TASK_LIB) $(AB2_LIBS)

export LD_LIBRARY_PATH=$(LD_LIBRARY_PATH):$(GCC_LIB_DIR):$(GCC_LIB64_DIR)

