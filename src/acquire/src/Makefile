include ../../../rules.mk

srcs = $(wildcard *.cpp)
objs = $(srcs:.cpp=.o)

bin  = $(BIN_DIR)/HAcquire
baselib = $(LIB_DIR)/libbase.a

.PHONY: all clean
all: $(bin)
	@echo "All made!"

$(bin): $(objs) $(baselib)
	$(CPP) $(CPP_FLAG) -o $@ $^ $(LIBS)

.cpp.o:
	$(CPP) $(CPP_FLAG) $(INCLS) -c $<

clean:
	@echo "Deleting $(bin)"
	@$(RM) $(bin)
	@echo "Deleting $(objs)"
	@$(RM) $(objs)

