BIN_DIR = ../../../bin
LIB_DIR = ../../../lib

################################################################
APP_INCL  = -I../include
BASE_INCL = -I../../base/include
XDBO_INCL = -I$(XDBO2_HOME)/include
JNI_INCL  = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
HDFS_INCL = -I$(HADOOP_HOME)/include

################################################################
BASE_LIB = -L$(LIB_DIR) -lbase
XDBO_LIB = -L$(XDBO2_HOME)/lib -lxdbo2
JNI_LIB  = -L$(JAVA_HOME)/jre/lib/amd64/server -ljvm
HDFS_LIB = -L$(HADOOP_HOME)/lib/native -lhdfs

################################################################
CPP = GNU_G++
CPP_FLAG = -g -m64 -Wall -O2 -DLINUX

################################################################
AR = ar
AR_FLAG = rv

################################################################
CP = cp
MV = mv
RM = rm -f

################################################################
INCLS = $(APP_INCL) $(BASE_INCL) $(XDBO_INCL) $(JNI_INCL) $(HDFS_INCL)
LIBS  = $(BASE_LIB) $(XDBO_LIB) $(JNI_LIB) $(HDFS_LIB)

