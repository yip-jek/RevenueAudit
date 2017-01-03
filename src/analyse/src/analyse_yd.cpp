#include "analyse_yd.h"

Analyse_YD::Analyse_YD()
{
	m_sType = "一点稽核";
}

Analyse_YD::~Analyse_YD()
{
}

std::string Analyse_YD::GetLogFilePrefix()
{
	return std::string("Analyse_YD");
}

