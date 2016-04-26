#pragma once

#include <string>

namespace base
{

class PubTime
{
public:
	// 今天之后的第n天, 返回格式：YYYYMMDD
	static std::string DateNowPlusDays(unsigned int days);
	// 今天之前的第n天, 返回格式：YYYYMMDD
	static std::string DateNowMinusDays(unsigned int days);

	// 这个月之后的第n个月, 返回格式：YYYYMM
	static std::string DateNowPlusMonths(unsigned int months);
	// 这个月之前的第n个月, 返回格式：YYYYMM
	static std::string DateNowMinusMonths(unsigned int months);
};

}	// namespace base

