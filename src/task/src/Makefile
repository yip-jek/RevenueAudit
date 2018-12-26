include ../../../rules.mk

INCLS = $(APP_INCL) $(BASE_INCL) $(XDBO_INCL)
LIBS  = $(SYS_LIB) $(BASE_LIB) $(XDBO_LIB)

srcs = $(wildcard *.cpp)
objs = $(srcs:.cpp=.o)

baselib = $(LIB_DIR)/libbase.a
bin     = $(BIN_DIR)/Task

.PHONY: all clean
all: $(bin)
	@echo "All made!"

$(bin): $(objs) $(baselib)
	$(CPP) $(CPP_FLAG) -o $@ $(objs) $(LIBS)

.cpp.o:
	$(CPP) $(CPP_FLAG) $(INCLS) -c $<

clean:
	@echo "[CLEAN] Deleting $(bin)"
	@$(RM) $(bin)
	@echo "[CLEAN] Deleting $(objs)"
	@$(RM) $(objs)

