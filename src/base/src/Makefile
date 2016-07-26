include ../../../rules.mk

srcs = $(wildcard *.cpp)
objs = $(srcs:.cpp=.o)

lib      = $(LIB_DIR)/libbase.a
lib_test = $(LIB_DIR)/libbase_test.a

.PHONY: all test allclean clean testclean
all: $(lib)
	@echo "All made!"

test: CPP_FLAG += -DTEST
test: $(lib_test)
	@echo "All test made!"

$(lib): $(objs)
	$(AR) $(AR_FLAG) $@ $^

$(lib_test): $(objs)
	$(AR) $(AR_FLAG) $@ $^

.cpp.o:
	$(CPP) $(CPP_FLAG) $(INCLS) -c $<

allclean:
	@echo "[ALLCLEAN] Deleting $(lib) $(lib_test)"
	@$(RM) $(lib) $(lib_test)
	@echo "[ALLCLEAN] Deleting $(objs)"
	@$(RM) $(objs)

clean:
	@echo "[CLEAN] Deleting $(lib)"
	@$(RM) $(lib)
	@echo "[CLEAN] Deleting $(objs)"
	@$(RM) $(objs)

testclean:
	@echo "[TESTCLEAN] Deleting $(lib_test)"
	@$(RM) $(lib_test)
	@echo "[TESTCLEAN] Deleting $(objs)"
	@$(RM) $(objs)

