include ../../rules.mk

incl = -I../include
libs = 

srcs = $(wildcard *.cpp)
objs = $(srcs:.cpp=.o)

bin  = $(BIN_DIR)/hz_acquire

.PHONY: all
all: $(bin)
	@echo "All made!"

$(bin): $(objs)
	$(CPP) $(CPP_FLAG) -o $@ $^ $(libs)

.cpp.o:
	$(CPP) $(CPP_FLAG) $(incl) -c $<

clean:
	@echo "Deleting $(bin)"
	@$(RM) $(bin)
	@echo "Deleting $(objs)"
	@$(RM) $(objs)

