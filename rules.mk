BIN_DIR    = ../../../bin
LIB_DIR    = ../../../lib
LOCAL_DIR  = /usr/local

################################################################
APP_INCL    = -I../include
BASE_INCL   = -I../../base/include
XDBO_INCL   = -I$(RA_ROOT)/include
THRIFT_INCL = -I../../thrift_hive/include -I$(LOCAL_DIR)/include/thrift
HDFS_INCL   = -I$(HADOOP_HOME)/include

################################################################
BASE_LIB    = -L$(LIB_DIR) -lbase
XDBO_LIB    = -L$(RA_ROOT)/lib -lxdbo2
THRIFT_LIB  = -L$(LOCAL_DIR)/lib -lthrift -L$(LIB_DIR) -lthrift_hive
HDFS_LIB    = -L$(HADOOP_HOME)/lib/native -lhdfs

################################################################
CPP = g++
CPP_FLAG = -g -m64 -Wall -O2 -DLINUX

################################################################
AR = ar
AR_FLAG = rv

################################################################
CP = cp
MV = mv
RM = rm -f

################################################################
INCLS = $(APP_INCL) $(BASE_INCL) $(XDBO_INCL) $(THRIFT_INCL) $(HDFS_INCL)
LIBS  = $(BASE_LIB) $(XDBO_LIB) $(THRIFT_LIB) $(HDFS_LIB)

