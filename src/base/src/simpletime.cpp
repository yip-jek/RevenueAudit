#include "simpletime.h"
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>

namespace base
{

bool operator > (const SimpleTime& st_l, const SimpleTime& st_r)
{
	const long long L_TIME = st_l.GetTime();
	const long long R_TIME = st_r.GetTime();
	return (L_TIME > R_TIME || (L_TIME == R_TIME && st_l.usec > st_r.usec));
}

bool operator < (const SimpleTime& st_l, const SimpleTime& st_r)
{
	const long long L_TIME = st_l.GetTime();
	const long long R_TIME = st_r.GetTime();
	return (L_TIME < R_TIME || (L_TIME == R_TIME && st_l.usec < st_r.usec));
}

bool operator == (const SimpleTime& st_l, const SimpleTime& st_r)
{
	return (st_l.valid == st_r.valid
		&& st_l.year == st_r.year
		&& st_l.mon  == st_r.mon
		&& st_l.day  == st_r.day
		&& st_l.hour == st_r.hour
		&& st_l.min  == st_r.min
		&& st_l.sec  == st_r.sec
		&& st_l.usec == st_r.usec);
}

bool operator != (const SimpleTime& st_l, const SimpleTime& st_r)
{
	return !(st_l == st_r);
}

bool operator >= (const SimpleTime& st_l, const SimpleTime& st_r)
{
	return (st_l > st_r || st_l == st_r);
}

bool operator <= (const SimpleTime& st_l, const SimpleTime& st_r)
{
	return (st_l < st_r || st_l == st_r);
}

SimpleTime::SimpleTime()
{
	Init();
}

SimpleTime::SimpleTime(int y, int m, int d, int h, int mi, int s, int us /*= 0*/)
{
	Set(y, m, d, h, mi, s, us);
}

SimpleTime::SimpleTime(long long time)
{
	Set(time);
}

SimpleTime::SimpleTime(const SimpleTime& st)
:valid(st.valid)
,year(st.year)
,mon(st.mon)
,day(st.day)
,hour(st.hour)
,min(st.min)
,sec(st.sec)
,usec(st.usec)
{
}

SimpleTime::~SimpleTime()
{
}

const SimpleTime& SimpleTime::operator = (const SimpleTime& st)
{
	if ( this != &st )
	{
		this->valid = st.valid;
		this->year  = st.year;
		this->mon   = st.mon ;
		this->day   = st.day ;
		this->hour  = st.hour;
		this->min   = st.min ;
		this->sec   = st.sec ;
		this->usec  = st.usec;
	}

	return *this;
}

SimpleTime SimpleTime::Now()
{
	struct timeval tv_now;
	gettimeofday(&tv_now, NULL);
	tm* pt = localtime(&tv_now.tv_sec);

	return SimpleTime(pt->tm_year+1900, pt->tm_mon+1, pt->tm_mday, pt->tm_hour, pt->tm_min, pt->tm_sec, tv_now.tv_usec);
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
		return 0;
	}
}

long long SimpleTime::GetTime() const
{
	long long ll_time = 0;
	ll_time += year * 10000000000;
	ll_time += mon  * 100000000;
	ll_time += day  * 1000000;
	ll_time += hour * 10000;
	ll_time += min  * 100;
	ll_time += sec;
	return ll_time;
}

std::string SimpleTime::TimeStamp() const
{
	return TimeFormat("%04d-%02d-%02d %02d:%02d:%02d", year, mon, day, hour, min, sec);
}

std::string SimpleTime::LTimeStamp() const
{
	return TimeFormat("%04d-%02d-%02d %02d:%02d:%02d.%03d", year, mon, day, hour, min, sec, (usec/1000));
}

std::string SimpleTime::LLTimeStamp() const
{
	return TimeFormat("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, mon, day, hour, min, sec, usec);
}

std::string SimpleTime::Time14() const
{
	return TimeFormat("%04d%02d%02d%02d%02d%02d", year, mon, day, hour, min, sec);
}

std::string SimpleTime::Time17() const
{
	return TimeFormat("%04d%02d%02d%02d%02d%02d%03d", year, mon, day, hour, min, sec, (usec/1000));
}

std::string SimpleTime::Time20() const
{
	return TimeFormat("%04d%02d%02d%02d%02d%02d%06d", year, mon, day, hour, min, sec, usec);
}

std::string SimpleTime::DayTime8() const
{
	return TimeFormat("%04d%02d%02d", year, mon, day);
}

std::string SimpleTime::DayTime10() const
{
	return TimeFormat("%04d-%02d-%02d", year, mon, day);
}

std::string SimpleTime::MonTime6() const
{
	return TimeFormat("%04d%02d", year, mon);
}

std::string SimpleTime::MonTime7() const
{
	return TimeFormat("%04d-%02d", year, mon);
}

std::string SimpleTime::YearTime() const
{
	return TimeFormat("%04d", year);
}

void SimpleTime::Init()
{
	valid = false;
	year  = 0;
	mon   = 0;
	day   = 0;
	hour  = 0;
	min   = 0;
	sec   = 0;
	usec  = 0;
}

bool SimpleTime::Set(int y, int m, int d, int h, int mi, int s, int us /*= 0*/)
{
	Init();

	int s_y  = 0;
	int s_m  = 0;
	int s_d  = 0;
	int s_h  = 0;
	int s_mi = 0;
	int s_s  = 0;
	int s_us = 0;

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

	if ( h < 0 || h > 23 )		// 无效小时
	{
		return false;
	}
	s_h = h;

	if ( mi < 0 || mi > 59 )	// 无效分钟
	{
		return false;
	}
	s_mi = mi;

	if ( s < 0 || s > 59 )		// 无效秒
	{
		return false;
	}
	s_s = s;

	if ( us < 0 || us > 999999 )	// 无效微秒
	{
		return false;
	}
	s_us = us;

	year  = s_y ;
	mon   = s_m ;
	day   = s_d ;
	hour  = s_h ;
	min   = s_mi;
	sec   = s_s ;
	usec  = s_us;
	return (valid = true);
}

bool SimpleTime::Set(long long time)
{
	int y  = time / 10000000000;
	int m  = (time % 10000000000) / 100000000;
	int d  = (time % 100000000) / 1000000;
	int h  = (time % 1000000) / 10000;
	int mi = (time % 10000) / 100;
	int s  = time % 100;

	return Set(y, m, d, h, mi, s);
}

std::string SimpleTime::TimeFormat(const char* format, ...)
{
	char buf[64] = "";
	va_list arg_ptr;
	va_start(arg_ptr, format);
	vsprintf(buf, format, arg_ptr);
	va_end(arg_ptr);
	return buf;
}

}	// namespace base

