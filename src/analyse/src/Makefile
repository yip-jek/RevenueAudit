include ../../../rules.mk

srcs = $(wildcard *.cpp)
objs = $(srcs:.cpp=.o)

baselib     = $(LIB_DIR)/libbase.a
basetestlib = $(LIB_DIR)/libbase_test.a

bin      = $(BIN_DIR)/HAnalyse
bin_test = $(BIN_DIR)/HAnalyse.test

.PHONY: all test allclean clean testclean
all: $(bin)
	@echo "All made!"

test: CPP_FLAG += -DTEST
test: BASE_LIB = -L$(LIB_DIR) -lbase_test
test: $(bin_test)
	@echo "All test made!"

$(bin): $(objs) $(baselib)
	$(CPP) $(CPP_FLAG) -o $@ $(objs) $(LIBS)

$(bin_test): $(objs) $(basetestlib)
	$(CPP) $(CPP_FLAG) -o $@ $(objs) $(LIBS)

.cpp.o:
	$(CPP) $(CPP_FLAG) $(INCLS) -c $<

allclean:
	@echo "[ALLCLEAN] Deleting $(bin) $(bin_test)"
	@$(RM) $(bin) $(bin_test)
	@echo "[ALLCLEAN] Deleting $(objs)"
	@$(RM) $(objs)

clean:
	@echo "[CLEAN] Deleting $(bin)"
	@$(RM) $(bin)
	@echo "[CLEAN] Deleting $(objs)"
	@$(RM) $(objs)

testclean:
	@echo "[TESTCLEAN] Deleting $(bin_test)"
	@$(RM) $(bin_test)
	@echo "[TESTCLEAN] Deleting $(objs)"
	@$(RM) $(objs)

