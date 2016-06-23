#include "simpletime.h"
#include <stdio.h>
#include <stdarg.h>
#include <time.h>

namespace base
{

SimpleTime::SimpleTime()
:year(0), mon(0), day(0), hour(0), min(0), sec(0)
{
}

SimpleTime::SimpleTime(int y, int m, int d, int h, int mi, int s)
:year(0), mon(0), day(0), hour(0), min(0), sec(0)
{
	Init(y, m, d, h, mi, s);
}

SimpleTime::SimpleTime(const SimpleTime& st)
:year(st.year), mon(st.mon), day(st.day), hour(st.hour), min(st.min), sec(st.sec)
{
}

SimpleTime::~SimpleTime()
{
}

const SimpleTime& SimpleTime::operator = (const SimpleTime& st)
{
	if ( this != &st )
	{
		this->year = st.year;
		this->mon  = st.mon ;
		this->day  = st.day ;
		this->hour = st.hour;
		this->min  = st.min ;
		this->sec  = st.sec ;
	}

	return *this;
}

SimpleTime SimpleTime::Now()
{
	time_t t = time(NULL);
	tm* pt = localtime(&t);

	return SimpleTime(pt->tm_year+1900, pt->tm_mon+1, pt->tm_mday, pt->tm_hour, pt->tm_min, pt->tm_sec);
}

bool SimpleTime::IsLeapYear(int year)
{
	if ( year > 0 )
	{
		return (year%4 == 0 && year%100 != 0) || (year%400 == 0);
	}

	return false;
}

int SimpleTime::LastDayOfTheMon(int year, int mon)
{
	switch ( mon )
	{
	case 1:			// 大月
	case 3:
	case 5:
	case 7:
	case 8:
	case 10:
	case 12:
		return 31;
	case 4:			// 小月
	case 6:
	case 9:
	case 11:
		return 30;
	case 2:
		if ( IsLeapYear(year) )		// 闰年
		{
			return 29;
		}
		else	// 平年
		{
			return 28;
		}
		break;
	default:	// 无效月份
		return -1;
	}
}

int SimpleTime::GetYear() const
{
	return year;
}

int SimpleTime::GetMon() const
{
	return mon;
}

int SimpleTime::GetDay() const
{
	return day;
}

int SimpleTime::GetHour() const
{
	return hour;
}

int SimpleTime::GetMin() const
{
	return min;
}

int SimpleTime::GetSec() const
{
	return sec;
}

std::string SimpleTime::TimeStamp()
{
	return TimeFormat("%04d-%02d-%02d %02d:%02d:%02d", year, mon, day, hour, min, sec);
}

std::string SimpleTime::Time14()
{
	return TimeFormat("%04d%02d%02d%02d%02d%02d", year, mon, day, hour, min, sec);
}

std::string SimpleTime::DayTime8()
{
	return TimeFormat("%04d%02d%02d", year, mon, day);
}

std::string SimpleTime::DayTime10()
{
	return TimeFormat("%04d-%02d-%02d", year, mon, day);
}

std::string SimpleTime::MonTime6()
{
	return TimeFormat("%04d%02d", year, mon);
}

std::string SimpleTime::MonTime7()
{
	return TimeFormat("%04d-%02d", year, mon);
}

std::string SimpleTime::YearTime()
{
	return TimeFormat("%04d", year);
}

bool SimpleTime::Init(int y, int m, int d, int h, int mi, int s)
{
	int s_y  = 0;
	int s_m  = 0;
	int s_d  = 0;
	int s_h  = 0;
	int s_mi = 0;
	int s_s  = 0;

	if ( y <= 0 )	// 无效年份
	{
		return false;
	}
	s_y = y;

	if ( m < 1 || m > 12 )	// 无效月份
	{
		return false;
	}
	s_m = m;

	if ( d < 1 || d > LastDayOfTheMon(y, m) )	// 无效日份
	{
		return false;
	}
	s_d = d;

	if ( h < 0 || h > 23 )	// 无效小时
	{
		return false;
	}
	s_h = h;

	if ( mi < 0 || mi > 59 )	// 无效分钟
	{
		return false;
	}
	s_mi = mi;

	if ( s < 0 || s > 59 )	// 无效秒
	{
		return false;
	}
	s_s = s;

	year = s_y ;
	mon  = s_m ;
	day  = s_d ;
	hour = s_h ;
	min  = s_mi;
	sec  = s_s ;
	return true;
}

std::string SimpleTime::TimeFormat(const char* format, ...)
{
	if ( NULL == format )
	{
		return std::string();
	}

	char buf[32] = "";
	va_list arg_ptr;
	va_start(arg_ptr, format);
	vsprintf(buf, format, arg_ptr);
	va_end(arg_ptr);
	return buf;
}

}	// namespace base

