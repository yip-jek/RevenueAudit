#include "pubtime.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "pubstr.h"

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
	case PubTime::DT_UNKNOWN:
	default:
		return "UNKNOWN";
	}
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

	std::string str_mon = boost::gregorian::to_iso_string(now.date());
	return str_mon.substr(0, 6);
}

std::string PubTime::DateNowMinusMonths(unsigned int months)
{
	boost::posix_time::ptime now(boost::posix_time::second_clock::local_time());

	now = now - boost::gregorian::months(months);

	std::string str_mon = boost::gregorian::to_iso_string(now.date());
	return str_mon.substr(0, 6);
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
	try
	{
		std::string& ref_off = vec_fmt[1];
		boost::trim(ref_off);

		date_off = boost::lexical_cast<unsigned int>(ref_off);
	}
	catch ( boost::bad_lexical_cast& ex )	// 转换失败
	{
		return false;
	}

	std::string& ref_flag = vec_fmt[0];

	boost::trim(ref_flag);
	boost::to_upper(ref_flag);

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

}	// namespace base

