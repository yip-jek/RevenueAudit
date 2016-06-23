#pragma once

#include <string>

namespace base
{

// 简单时间类
class SimpleTime
{
public:
	SimpleTime();
	SimpleTime(int y, int m, int d, int h, int mi, int s);
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
	int GetYear() const;
	int GetMon() const;
	int GetDay() const;
	int GetHour() const;
	int GetMin() const;
	int GetSec() const;

	// 时间戳
	std::string TimeStamp();

	// 时间格式：YYYYMMDDHHMISS
	std::string Time14();

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
	bool Init(int y, int m, int d, int h, int mi, int s);

	// 格式化时间字符串
	std::string TimeFormat(const char* format, ...);

private:
	int year;
	int mon;
	int day;
	int hour;
	int min;
	int sec;
};

}	// namespace base

