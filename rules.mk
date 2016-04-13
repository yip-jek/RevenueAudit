BIN_DIR    = ../../../bin
LIB_DIR    = ../../../lib
LOCAL_DIR  = /usr/local

################################################################
APP_INCL    = -I../include
BASE_INCL   = -I../../base/include
THRIFT_INCL = -I../../thrift_hive/include -I$(LOCAL_DIR)/include/thrift

################################################################
BASE_LIB    = -L$(LIB_DIR) -lbase

THRIFT_LIB  = -L$(LOCAL_DIR)/lib -lthrift -L$(LIB_DIR) -lthrift_hive

################################################################
ifeq ($(shell uname -m), i686) # 32 bit OS
OS_BITS = 32
else # 64 bit OS
OS_BITS = 64
endif

CPP = g++
CPP_FLAG = -g -m$(OS_BITS) -Wall -O2 -DLINUX

################################################################
AR = ar
AR_FLAG = rv

################################################################
CP = cp
MV = mv
RM = rm -f

################################################################
INCLS = $(APP_INCL) $(BASE_INCL) $(THRIFT_INCL)
LIBS  = $(BASE_LIB) $(THRIFT_LIB)

