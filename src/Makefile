DIRS = base acquire analyse

.PHONY: all test allclean clean testclean

all:
	@for i in $(DIRS); do make -C $$i; done

test:
	@for i in $(DIRS); do make -C $$i test; done

allclean:
	@for i in $(DIRS); do make -C $$i allclean; done

clean:
	@for i in $(DIRS); do make -C $$i clean; done

testclean:
	@for i in $(DIRS); do make -C $$i testclean; done

