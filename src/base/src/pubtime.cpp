#include "pubtime.h"
#include "pubstr.h"
#include "simpletime.h"

namespace base
{

std::string PubTime::DateType2String(PubTime::DATE_TYPE dt)
{
	switch ( dt )
	{
	case PubTime::DT_DAY:
		return "DAY";
	case PubTime::DT_MONTH:
		return "MONTH";
	//case PubTime::DT_YEAR:
	//	return "YEAR";
	case PubTime::DT_UNKNOWN:
	default:
		return "UNKNOWN";
	}
}

std::string PubTime::TheDatePlusDays(int year, int mon, int day, unsigned int days)
{
	return TheDateDays_S(year, mon, day, days, true);
}

std::string PubTime::TheDateMinusDays(int year, int mon, int day, unsigned int days)
{
	return TheDateDays_S(year, mon, day, days, false);
}

std::string PubTime::TheDatePlusMonths(int year, int mon, unsigned int months)
{
	return TheDateMonths_S(year, mon, months, true);
}

std::string PubTime::TheDateMinusMonths(int year, int mon, unsigned int months)
{
	return TheDateMonths_S(year, mon, months, false);
}

std::string PubTime::DateNowPlusDays(unsigned int days)
{
	boost::posix_time::ptime now(boost::posix_time::second_clock::local_time());

	now = now + boost::gregorian::days(days);

	return boost::gregorian::to_iso_string(now.date());
}

std::string PubTime::DateNowMinusDays(unsigned int days)
{
	boost::posix_time::ptime now(boost::posix_time::second_clock::local_time());

	now = now - boost::gregorian::days(days);

	return boost::gregorian::to_iso_string(now.date());
}

std::string PubTime::DateNowPlusMonths(unsigned int months)
{
	boost::posix_time::ptime now(boost::posix_time::second_clock::local_time());

	now = now + boost::gregorian::months(months);

	return boost::gregorian::to_iso_string(now.date()).substr(0, 6);
}

std::string PubTime::DateNowMinusMonths(unsigned int months)
{
	boost::posix_time::ptime now(boost::posix_time::second_clock::local_time());

	now = now - boost::gregorian::months(months);

	return boost::gregorian::to_iso_string(now.date()).substr(0, 6);
}

long PubTime::DayApartFromToday(int year, int mon, int day)
{
	boost::gregorian::date the_day(year, mon, day);
	boost::gregorian::date today(boost::gregorian::day_clock::local_day());
	boost::gregorian::date_duration day_apart = today - the_day;
	return day_apart.days();
}

bool PubTime::DateApartFromNow(const std::string& fmt, PubTime::DATE_TYPE& d_type, std::string& date)
{
	// 默认：未知类型
	d_type = DT_UNKNOWN;

	bool is_plus = true;
	std::vector<std::string> vec_fmt;

	const std::string PLUS  = "+";		// 加号
	const std::string MINUS = "-";		// 减号

	if ( fmt.find(PLUS) != std::string::npos )
	{
		is_plus = true;

		PubStr::Str2StrVector(fmt, PLUS, vec_fmt);
	}
	else if ( fmt.find(MINUS) != std::string::npos )
	{
		is_plus = false;

		PubStr::Str2StrVector(fmt, MINUS, vec_fmt);
	}
	else	// 格式有误：找不到运算符
	{
		return false;
	}

	// 格式错误！
	if ( vec_fmt.size() != 2 )
	{
		return false;
	}

	// 获取时间偏移量
	unsigned int date_off = 0;
	if ( !PubStr::Str2UInt(PubStr::TrimB(vec_fmt[1]), date_off) )	// 转换失败
	{
		return false;
	}

	std::string& ref_flag = vec_fmt[0];
	PubStr::TrimUpper(ref_flag);

	if ( "DAY" == ref_flag )			// 日
	{
		d_type = DT_DAY;

		if ( is_plus )
		{
			date = PubTime::DateNowPlusDays(date_off);
		}
		else
		{
			date = PubTime::DateNowMinusDays(date_off);
		}
	}
	else if ( "MON" == ref_flag )		// 月
	{
		d_type = DT_MONTH;

		if ( is_plus )
		{
			date = PubTime::DateNowPlusMonths(date_off);
		}
		else
		{
			date = PubTime::DateNowMinusMonths(date_off);
		}
	}
	else	// 无法识别的采集时间标识
	{
		return false;
	}

	return true;
}

bool PubTime::TheDateOf(const std::string& date_of_what, std::string& date)
{
	std::string d_of_what = PubStr::TrimUpperB(date_of_what);

	if ( "TODAY_OF_LAST_WEEK" == d_of_what )			// 上个星期的今天
	{
		date = DateNowMinusDays(7);
	}
	else if ( "TODAY_OF_LAST_MONTH" == d_of_what )		// 上个月的今天
	{
		SimpleTime st_now = SimpleTime::Now();
		int year = st_now.GetYear();
		int mon  = st_now.GetMon();
		int day  = st_now.GetDay();

		if ( 1 == mon )		// 本月是一月份
		{
			--year;
			mon = 12;
		}
		else	// 本月不是一月份
		{
			--mon;
		}

		// 是否大于上个月的日份
		int last_day = SimpleTime::LastDayOfTheMon(year, mon);
		if ( day > last_day )
		{
			day = last_day;
		}

		date = SimpleTime(year, mon, day, 0, 0, 0).DayTime8();
	}
	else if ( "TODAY_OF_LAST_YEAR" == d_of_what )		// 去年的今天
	{
		SimpleTime st_now = SimpleTime::Now();
		int year = st_now.GetYear() - 1;
		int mon  = st_now.GetMon();
		int day  = st_now.GetDay();

		// 是否大于去年的日份
		int last_day = SimpleTime::LastDayOfTheMon(year, mon);
		if ( day > last_day )
		{
			day = last_day;
		}

		date = SimpleTime(year, mon, day, 0, 0, 0).DayTime8();
	}
	else if ( "THIS_MONTH_OF_LAST_YEAR" == d_of_what )	// 去年的这个月
	{
		SimpleTime st_now = SimpleTime::Now();
		int year = st_now.GetYear() - 1;
		int mon  = st_now.GetMon();

		date = SimpleTime(year, mon, 0, 0, 0, 0).MonTime6();
	}
	else	// 不支持的输入类型
	{
		return false;
	}

	return true;
}

std::string PubTime::TheDateDays_S(int year, int mon, int day, unsigned int days, bool is_plus)
{
	int last_day = SimpleTime::LastDayOfTheMon(year, mon);
	if ( year < MIN_YEAR || mon < 1 || mon > 12 || day < 1 || day > last_day ) // Invalid
	{
		return std::string();
	}

	int new_day = 0;
	if ( is_plus )
	{
		new_day = day + days;
	}
	else
	{
		new_day = day - days;
	}

	int new_year = year;
	int new_mon  = mon;
	while ( true )
	{
		if ( new_day > last_day )
		{
			new_day -= last_day;

			++new_mon;
			if ( new_mon > 12 )
			{
				new_mon = 1;
				++new_year;
			}

			last_day = SimpleTime::LastDayOfTheMon(new_year, new_mon);
		}
		else if ( new_day < 0 )
		{
			new_day += last_day;

			--new_mon;
			if ( new_mon < 1 )
			{
				new_mon = 12;
				--new_year;
			}

			last_day = SimpleTime::LastDayOfTheMon(new_year, new_mon);
		}
		else if ( 0 == new_day )
		{
			--new_mon;
			if ( new_mon < 1 )
			{
				new_mon = 12;
				--new_year;
			}

			new_day = SimpleTime::LastDayOfTheMon(new_year, new_mon);
			break;
		}
		else	// new_day >= 1 && new_day <= last_day
		{
			break;
		}
	}

	if ( new_year < MIN_YEAR )
	{
		return std::string();
	}

	std::string str_date;
	PubStr::SetFormatString(str_date, "%04d%02d%02d", new_year, new_mon, new_day);
	return str_date;
}

std::string PubTime::TheDateMonths_S(int year, int mon, unsigned int months, bool is_plus)
{
	if ( year < MIN_YEAR || mon < 1 || mon > 12 )	// Invalid
	{
		return std::string();
	}

	int total_m = 0;
	if ( is_plus )
	{
		total_m = year * 12 + mon + months;
	}
	else
	{
		total_m = year * 12 + mon - months;
	}

	if ( total_m <= (MIN_YEAR*12) )
	{
		return std::string();
	}

	int new_year = total_m / 12;
	int new_mon  = total_m % 12;
	if ( 0 == new_mon )
	{
		--new_year;
		new_mon = 12;
	}

	std::string str_mon;
	PubStr::SetFormatString(str_mon, "%04d%02d", new_year, new_mon);
	return str_mon;
}

}	// namespace base

