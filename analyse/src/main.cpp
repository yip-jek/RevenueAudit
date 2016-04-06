#include <iostream>
#include "test.h"

int main(int argc, char* argv[])
{
	std::cout << "[main] " << argv[0] << " ... OK!" << std::endl;

	Test tt;
	std::cout << "TEST> before [INIT] value: " << tt.GetVal() << std::endl;
	tt.Init();
	std::cout << "TEST> after [INIT] value: " << tt.GetVal() << std::endl;
	return 0;
}

