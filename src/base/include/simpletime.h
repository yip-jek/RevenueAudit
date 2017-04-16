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
	explicit SimpleTime(long long time);		// 时间格式：YYYYMMDDHHMISS
	SimpleTime(const SimpleTime& st);
	virtual ~SimpleTime();

	const SimpleTime& operator = (const SimpleTime& st);

	friend bool operator > (const SimpleTime& st_l, const SimpleTime& st_r);
	friend bool operator < (const SimpleTime& st_l, const SimpleTime& st_r);
	friend bool operator == (const SimpleTime& st_l, const SimpleTime& st_r);
	friend bool operator != (const SimpleTime& st_l, const SimpleTime& st_r);
	friend bool operator >= (const SimpleTime& st_l, const SimpleTime& st_r);
	friend bool operator <= (const SimpleTime& st_l, const SimpleTime& st_r);

public:
	// 当前时间
	static SimpleTime Now();

	// 当前时间 (time_t)
	static time_t CurrentTime();

	// 是否为闰年
	static bool IsLeapYear(int year);

	// 本月最后一天
	// 返回：大月-31, 小月-30，闰年2月-29，平年2月-28, 其他（错误）-1
	static int LastDayOfTheMon(int year, int mon);

public:
	// 是否有效？
	bool IsValid() const { return valid; }

	int GetYear() const { return year; }
	int GetMon()  const { return mon ; }
	int GetDay()  const { return day ; }
	int GetHour() const { return hour; }
	int GetMin()  const { return min ; }
	int GetSec()  const { return sec ; }
	int GetUSec() const { return usec; }

	// 时间格式：YYYYMMDDHHMISS
	long long GetTime() const;

	// 设置时间
	bool Set(int y, int m, int d, int h, int mi, int s, int us = 0);
	bool Set(long long time);		// 时间格式：YYYYMMDDHHMISS

	// 时间戳
	std::string TimeStamp() const;

	// 长时间戳
	std::string LTimeStamp() const;

	// 超长时间戳
	std::string LLTimeStamp() const;

	// 时间格式：YYYYMMDDHHMISS
	std::string Time14() const;

	// 时间格式：YYYYMMDDHHMISSUS3
	std::string Time17() const;

	// 时间格式：YYYYMMDDHHMISSUS6
	std::string Time20() const;

	// 日时间，格式：YYYYMMDD
	std::string DayTime8() const;

	// 日时间，格式：YYYY-MM-DD
	std::string DayTime10() const;

	// 月时间，格式：YYYYMM
	std::string MonTime6() const;

	// 月时间，格式：YYYY-MM
	std::string MonTime7() const;

	// 年时间，格式：YYYY
	std::string YearTime() const;

private:
	// 初始化
	void Init();

	// 格式化时间字符串
	static std::string TimeFormat(const char* format, ...);

private:
	bool valid;			// 有效位

	int year;
	int mon;
	int day;
	int hour;
	int min;
	int sec;
	int usec;
};

}	// namespace base

