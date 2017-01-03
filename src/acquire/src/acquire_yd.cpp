#include "acquire_yd.h"

Acquire_YD::Acquire_YD()
{
	m_sType = "一点稽核";
}

Acquire_YD::~Acquire_YD()
{
}

std::string Acquire_YD::GetLogFilePrefix()
{
	return std::string("Acquire_YD");
}

