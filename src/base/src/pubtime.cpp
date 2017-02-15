#include "pubtime.h"
#include "pubstr.h"

namespace base
{

const char* const PubTime::S_TODAY_OF_LAST_WEEK      = "TODAY_OF_LAST_WEEK";			// 上个星期的今天
const char* const PubTime::S_TODAY_OF_LAST_MONTH     = "TODAY_OF_LAST_MONTH";			// 上个月的今天
const char* const PubTime::S_TODAY_OF_LAST_YEAR      = "TODAY_OF_LAST_YEAR";			// 去年的今天
const char* const PubTime::S_THIS_MONTH_OF_LAST_YEAR = "THIS_MONTH_OF_LAST_YEAR";		// 去年的这个月

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
	const SimpleTime ST_NOW(SimpleTime::Now());
	return TheDateDays_S(ST_NOW.GetYear(), ST_NOW.GetMon(), ST_NOW.GetDay(), days, true);
}

std::string PubTime::DateNowMinusDays(unsigned int days)
{
	const SimpleTime ST_NOW(SimpleTime::Now());
	return TheDateDays_S(ST_NOW.GetYear(), ST_NOW.GetMon(), ST_NOW.GetDay(), days, false);
}

std::string PubTime::DateNowPlusMonths(unsigned int months)
{
	const SimpleTime ST_NOW(SimpleTime::Now());
	return TheDateMonths_S(ST_NOW.GetYear(), ST_NOW.GetMon(), months, true);
}

std::string PubTime::DateNowMinusMonths(unsigned int months)
{
	const SimpleTime ST_NOW(SimpleTime::Now());
	return TheDateMonths_S(ST_NOW.GetYear(), ST_NOW.GetMon(), months, false);
}

bool PubTime::SpreadTimeInterval(const DATE_TYPE& d_type, const std::string& time_intvl, const std::string& dim, std::vector<int>& vec_ts)
{
	std::vector<std::string> vec_str;
	PubStr::Str2StrVector(time_intvl, dim, vec_str);

	// 时间区间格式不正确
	if ( vec_str.size() != 2 )
	{
		return false;
	}

	int time_beg = 0;
	int time_end = 0;
	if ( !PubStr::Str2Int(vec_str[0], time_beg)
		|| !PubStr::Str2Int(vec_str[1], time_end)
		|| time_beg > time_end )
	{
		return false;
	}

	std::vector<int> vec_tmp;
	int time_cur = time_beg;
	int beg_year = 0;
	int beg_mon  = 0;
	int beg_day  = 0;
	int end_year = 0;
	int end_mon  = 0;
	int end_day  = 0;

	if ( DT_MONTH == d_type )	// 月
	{
		beg_year = time_beg / 100;
		beg_mon  = time_beg % 100;
		end_year = time_end / 100;
		end_mon  = time_end % 100;

		if ( beg_year <= 1970 || beg_year > 9999 || beg_mon < 1 || beg_mon > 12 
			|| end_year <= 1970 || end_year > 9999 || end_mon < 1 || end_mon > 12 )
		{
			return false;
		}

		while ( time_cur <= time_end )
		{
			vec_tmp.push_back(time_cur);

			if ( 12 == beg_mon )
			{
				++beg_year;
				beg_mon = 1;
			}
			else
			{
				++beg_mon;
			}
			time_cur = beg_year * 100 + beg_mon;
		}
	}
	else if ( DT_DAY == d_type )	// 日
	{
		beg_year = time_beg / 10000;
		beg_mon  = (time_beg % 10000) / 100;
		beg_day  = time_beg % 100;
		end_year = time_end / 10000;
		end_mon  = (time_end % 10000) / 100;
		end_day  = time_end % 100;

		if ( beg_year <= 1970 || beg_year > 9999 || beg_mon < 1 || beg_mon > 12 
			|| beg_day < 1 || beg_day > SimpleTime::LastDayOfTheMon(beg_year, beg_mon) 
			|| end_year <= 1970 || end_year > 9999 || end_mon < 1 || end_mon > 12 
			|| end_day < 1 || end_day > SimpleTime::LastDayOfTheMon(end_year, end_mon) )
		{
			return false;
		}

		while ( time_cur <= time_end )
		{
			vec_tmp.push_back(time_cur);

			if ( 31 == beg_day && 12 == beg_mon )
			{
				++beg_year;
				beg_mon = 1;
				beg_day = 1;
			}
			else if ( SimpleTime::LastDayOfTheMon(beg_year, beg_mon) == beg_day )
			{
				++beg_mon;
				beg_day = 1;
			}
			else
			{
				++beg_day;
			}
			time_cur = beg_year * 10000 + beg_mon * 100 + beg_day;
		}
	}
	else	// 不支持的时间类型
	{
		return false;
	}

	vec_tmp.swap(vec_ts);
	return true;
}

long PubTime::DayDifference(const base::SimpleTime& st_beg, const base::SimpleTime& st_end)
{
	if ( !st_beg.IsValid() || !st_end.IsValid() )
	{
		return 0;
	}

	return (SumDay_S(st_end.GetYear(), st_end.GetMon(), st_end.GetDay()) - SumDay_S(st_beg.GetYear(), st_beg.GetMon(), st_beg.GetDay()));
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

	if ( S_TODAY_OF_LAST_WEEK == d_of_what )			// 上个星期的今天
	{
		date = DateNowMinusDays(7);
	}
	else if ( S_TODAY_OF_LAST_MONTH == d_of_what )		// 上个月的今天
	{
		const SimpleTime ST_NOW = SimpleTime::Now();
		int year = ST_NOW.GetYear();
		int mon  = ST_NOW.GetMon();
		int day  = ST_NOW.GetDay();

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
	else if ( S_TODAY_OF_LAST_YEAR == d_of_what )		// 去年的今天
	{
		const SimpleTime ST_NOW = SimpleTime::Now();
		int year = ST_NOW.GetYear() - 1;
		int mon  = ST_NOW.GetMon();
		int day  = ST_NOW.GetDay();

		// 是否大于去年的日份
		int last_day = SimpleTime::LastDayOfTheMon(year, mon);
		if ( day > last_day )
		{
			day = last_day;
		}

		date = SimpleTime(year, mon, day, 0, 0, 0).DayTime8();
	}
	else if ( S_THIS_MONTH_OF_LAST_YEAR == d_of_what )	// 去年的这个月
	{
		const SimpleTime ST_NOW = SimpleTime::Now();
		int year = ST_NOW.GetYear() - 1;
		int mon  = ST_NOW.GetMon();

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
			--new_mon;
			if ( new_mon < 1 )
			{
				new_mon = 12;
				--new_year;
			}

			last_day = SimpleTime::LastDayOfTheMon(new_year, new_mon);
			new_day += last_day;
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

	int sum_mon = 0;
	if ( is_plus )
	{
		sum_mon = year * 12 + mon + months;
	}
	else
	{
		sum_mon = year * 12 + mon - months;
	}

	if ( sum_mon <= (MIN_YEAR*12) )
	{
		return std::string();
	}

	int new_year = sum_mon / 12;
	int new_mon  = sum_mon % 12;
	if ( 0 == new_mon )
	{
		--new_year;
		new_mon = 12;
	}

	std::string str_mon;
	PubStr::SetFormatString(str_mon, "%04d%02d", new_year, new_mon);
	return str_mon;
}

long PubTime::SumDay_S(int year, int mon, int day)
{
	const int FOUR_YEARS_DAY = 365 * 3 + 366;
	long sum_day = ((year - 1) / 4) * FOUR_YEARS_DAY + ((year - 1) % 4) * 365;
	for ( int i = 1; i < mon; ++i )
	{
		sum_day += SimpleTime::LastDayOfTheMon(year, i);
	}
	sum_day += day;
	return sum_day;
}

}	// namespace base

