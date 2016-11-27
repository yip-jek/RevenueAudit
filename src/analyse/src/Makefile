include ../../../rules.mk

srcs = $(wildcard *.cpp)
objs = $(srcs:.cpp=.o)

baselib = $(LIB_DIR)/libbase.a
bin     = $(BIN_DIR)/HAnalyse

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

