#pragma once

#include <string>

namespace base
{

// 简单时间类
class SimpleTime
{
public:
	SimpleTime();
	SimpleTime(int y, int m, int d, int h, int mi, int s, int us = 0);
	SimpleTime(const SimpleTime& st);
	virtual ~SimpleTime();

	const SimpleTime& operator = (const SimpleTime& st);

public:
	// 当前时间
	static SimpleTime Now();

	// 是否为闰年
	static bool IsLeapYear(int year);

	// 本月最后一天
	// 返回：大月-31, 小月-30，闰年2月-29，平年2月-28, 其他（错误）-1
	static int LastDayOfTheMon(int year, int mon);

public:
	int GetYear() const { return year; }
	int GetMon()  const { return mon ; }
	int GetDay()  const { return day ; }
	int GetHour() const { return hour; }
	int GetMin()  const { return min ; }
	int GetSec()  const { return sec ; }
	int GetUSec() const { return usec; }

	// 时间戳
	std::string TimeStamp();

	// 长时间戳
	std::string LTimeStamp();

	// 超长时间戳
	std::string LLTimeStamp();

	// 时间格式：YYYYMMDDHHMISS
	std::string Time14();

	// 时间格式：YYYYMMDDHHMISSUS3
	std::string Time17();

	// 时间格式：YYYYMMDDHHMISSUS6
	std::string Time20();

	// 日时间，格式：YYYYMMDD
	std::string DayTime8();

	// 日时间，格式：YYYY-MM-DD
	std::string DayTime10();

	// 月时间，格式：YYYYMM
	std::string MonTime6();

	// 月时间，格式：YYYY-MM
	std::string MonTime7();

	// 年时间，格式：YYYY
	std::string YearTime();

private:
	// 初始化时间
	bool Init(int y, int m, int d, int h, int mi, int s, int us);

	// 格式化时间字符串
	std::string TimeFormat(const char* format, ...);

private:
	int year;
	int mon;
	int day;
	int hour;
	int min;
	int sec;
	int usec;
};

}	// namespace base

