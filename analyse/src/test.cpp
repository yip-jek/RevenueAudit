#include "test.h"
#include <time.h>
#include <stdlib.h>

Test::Test()
:m_val(0)
{
	srand(time(0));
}

void Test::Init()
{
	m_val = rand() % 1000 + 1;
}

int Test::GetVal() const
{
	return m_val;
}

