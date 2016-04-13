include ../../../rules.mk

srcs = $(wildcard *.cpp)
objs = $(srcs:.cpp=.o)

lib  = $(LIB_DIR)/libbase.a

.PHONY: all clean
all: $(lib)
	@echo "All made!"

$(lib): $(objs)
	$(AR) $(AR_FLAG) $@ $^

.cpp.o:
	$(CPP) $(CPP_FLAG) $(INCLS) -c $<

clean:
	@echo "Deleting $(lib)"
	@$(RM) $(lib)
	@echo "Deleting $(objs)"
	@$(RM) $(objs)

