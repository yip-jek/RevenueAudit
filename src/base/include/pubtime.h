#pragma once

#include <string>

namespace base
{

class PubTime
{
public:
	// 时间类型
	enum DATE_TYPE
	{
		DT_UNKNOWN	= 0,		// 未知类型
		DT_DAY		= 1,		// 日-类型
		DT_MONTH	= 2,		// 月-类型
		//DT_YEAR	= 3,		// 年-类型
	};

	// 时间类型转换为字符串
	static std::string DateType2String(DATE_TYPE dt);

public:
	// 今天之后的第n天, 返回格式：YYYYMMDD
	static std::string DateNowPlusDays(unsigned int days);
	// 今天之前的第n天, 返回格式：YYYYMMDD
	static std::string DateNowMinusDays(unsigned int days);

	// 这个月之后的第n个月, 返回格式：YYYYMM
	static std::string DateNowPlusMonths(unsigned int months);
	// 这个月之前的第n个月, 返回格式：YYYYMM
	static std::string DateNowMinusMonths(unsigned int months);

	// 与当前时间相隔的时间（支持 月时间 或者 日时间）
	// fmt为时间格式：（月）[mon][+/-][月数]; （日）[day][+/-][日数]
	// 成功返回true，失败返回false
	static bool DateApartFromNow(const std::string& fmt, DATE_TYPE& d_type, std::string& date);

	// 特殊时间转换
	// 描述：...的时间(月时间或者日时间）
	// 返回：true-成功，false-失败
	// 支持的输入类型（date_of_what）：
	//   THE_DAY_OF_LAST_WEEK   - 上个星期的今天
	//   THE_DAY_OF_LAST_MONTH  - 上个月的今天
	//   THE_DAY_OF_LAST_YEAR   - 去年的今天
	//   THE_MONTH_OF_LAST_YEAR - 去年的这个月
	static bool TheDateOf(const std::string& date_of_what, std::string& date);
};

}	// namespace base

