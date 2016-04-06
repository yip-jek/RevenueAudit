#include <iostream>
#include "test.h"
#include "base.h"

int main(int argc, char* argv[])
{
	std::cout << "[main] " << argv[0] << " ... OK!" << std::endl;

	Test tt;
	std::cout << "TEST> before [INIT] value: " << tt.GetVal() << std::endl;
	tt.Init();
	std::cout << "TEST> after [INIT] value: " << tt.GetVal() << std::endl;

	std::cout << "base static val: " << Base::S_GetVal(1) << std::endl;

	Base b;
	std::cout << "base val: " << b.GetInterVal() << std::endl;
	return 0;
}

