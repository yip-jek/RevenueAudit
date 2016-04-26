#include "pubtime.h"
#include <boost/date_time/posix_time/posix_time.hpp>

namespace base
{

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

}	// namespace base

