#include <unistd.h>
#include "intervaltimer.h"
#include "pubstr.h"
#include "pubtime.h"

namespace base
{

const char* const IntervalTimer::S_MONTH_TYPE_W  = "MONTH";
const char* const IntervalTimer::S_MONTH_TYPE_F  = "YYYYMM";
const char* const IntervalTimer::S_DAY_TYPE_W    = "DAY";
const char* const IntervalTimer::S_DAY_TYPE_F    = "YYYYMMDD";
const char* const IntervalTimer::S_HOUR_TYPE_W   = "HOUR";
const char* const IntervalTimer::S_HOUR_TYPE_F   = "YYYYMMDDHH";
const char* const IntervalTimer::S_MINUTE_TYPE_W = "MINUTE";
const char* const IntervalTimer::S_MINUTE_TYPE_F = "YYYYMMDDHHMI";

IntervalTimer::IntervalTimer()
:m_bRunning(false)
,m_iType(IT_UNKNOWN)
,m_pfDifferTime(NULL)
,m_pThread(NULL)
{
}

IntervalTimer::~IntervalTimer()
{
}

bool IntervalTimer::IsIntervalTypeValid(IntervalType type)
{
	switch ( type )
	{
	case IT_MONTH:
	case IT_DAY:
	case IT_HOUR:
	case IT_MINUTE:
		return true;
	default:	// Unknown type !
		return false;
	}
}

int IntervalTimer::DifferMonthTime(const SimpleTime& st_beg, const SimpleTime& st_end)
{
	int beg_year = st_beg.GetYear();
	int beg_mon  = st_beg.GetMon();
	int end_year = st_end.GetYear();
	int end_mon  = st_end.GetMon();

	int diff_year = end_year - beg_year;
	int diff_mon  = (end_mon - beg_mon) + (diff_year * 12);

	return diff_mon;
}

int IntervalTimer::DifferDayTime(const SimpleTime& st_beg, const SimpleTime& st_end)
{
	return PubTime::DayDifference(st_beg, st_end);
}

int IntervalTimer::DifferHourTime(const SimpleTime& st_beg, const SimpleTime& st_end)
{
	int beg_hour = st_beg.GetHour();
	int end_hour = st_end.GetHour();

	int diff_day  = DifferDayTime(st_beg, st_end);
	int diff_hour = (end_hour - beg_hour) + (diff_day * 24);

	return diff_hour;
}

int IntervalTimer::DifferMinuteTime(const SimpleTime& st_beg, const SimpleTime& st_end)
{
	int beg_min = st_beg.GetMin();
	int end_min = st_end.GetMin();

	int diff_hour = DifferHourTime(st_beg, st_end);
	int diff_min  = (end_min - beg_min) + (diff_hour * 60);

	return diff_min;
}

bool IntervalTimer::Start()
{
	// 是否正在运行？
	if ( m_bRunning )
	{
		return false;
	}

	// 无效的时间间隔类型？
	if ( !IsIntervalTypeValid(m_iType) )
	{
		return false;
	}

	m_bRunning = true;
	m_sRecordTime = SimpleTime::Now();

	m_pThread  = new std::thread(&IntervalTimer::Run, this);
	if ( NULL == m_pThread )
	{
		m_bRunning = false;
	}

	return m_bRunning;
}

bool IntervalTimer::Stop()
{
	if ( !m_bRunning )
	{
		return false;
	}

	m_bRunning = false;
	if ( m_pThread != NULL )
	{
		m_pThread->join();
		delete m_pThread;
		m_pThread = NULL;
	}

	return m_bRunning;
}

bool IntervalTimer::IsRunning() const
{
	return m_bRunning;
}

bool IntervalTimer::SetIntervalType(IntervalType type)
{
	if ( IsIntervalTypeValid(type) )
	{
		m_iType = type;

		switch ( m_iType )
		{
		case IT_MONTH : m_pfDifferTime = &DifferMonthTime; break;
		case IT_DAY   : m_pfDifferTime = &DifferDayTime; break;
		case IT_HOUR  : m_pfDifferTime = &DifferHourTime; break;
		case IT_MINUTE: m_pfDifferTime = &DifferMinuteTime; break;
		default: m_pfDifferTime = NULL; break;
		}

		return true;
	}

	// 无效
	m_iType = IT_UNKNOWN;
	m_pfDifferTime = NULL;
	return false;
}

bool IntervalTimer::SetIntervalType(const std::string& str_type)
{
	const std::string TYPE = PubStr::TrimUpperB(str_type);

	if ( TYPE == S_MONTH_TYPE_W || TYPE == S_MONTH_TYPE_F )
	{
		SetIntervalType(IT_MONTH);
	}
	else if ( TYPE == S_DAY_TYPE_W || TYPE == S_DAY_TYPE_F )
	{
		SetIntervalType(IT_DAY);
	}
	else if ( TYPE == S_HOUR_TYPE_W || TYPE == S_HOUR_TYPE_F )
	{
		SetIntervalType(IT_HOUR);
	}
	else if ( TYPE == S_MINUTE_TYPE_W || TYPE == S_MINUTE_TYPE_F )
	{
		SetIntervalType(IT_MINUTE);
	}
	else
	{
		SetIntervalType(IT_UNKNOWN);
		return false;
	}

	return true;
}

std::string IntervalTimer::GetIntervalTypeTime() const
{
	switch ( m_iType )
	{
	case IT_MONTH:		// YYYYMM
		return m_sRecordTime.MonTime6();
	case IT_DAY:		// YYYYMMDD
		return m_sRecordTime.DayTime8();
	case IT_HOUR:		// YYYYMMDDHH
		return m_sRecordTime.Time14().substr(0, 10);
	case IT_MINUTE:		// YYYYMMDDHHMI
		return m_sRecordTime.Time14().substr(0, 12);
	default:	// Unknown type !
		return std::string();
	}
}

void IntervalTimer::Register(TimerInterface* pTimer)
{
	if ( pTimer != NULL )
	{
		m_sTimers.insert(pTimer);
	}
}

void IntervalTimer::Unregister(TimerInterface* pTimer)
{
	if ( pTimer != NULL && !m_sTimers.empty() )
	{
		for ( std::set<TimerInterface*>::iterator it = m_sTimers.begin(); it != m_sTimers.end(); ++it )
		{
			if ( pTimer == *it )
			{
				m_sTimers.erase(it);
				break;
			}
		}
	}
}

void IntervalTimer::Run()
{
	while ( m_bRunning )
	{
		if ( CheckTimeInterval() )
		{
			SendNotification();
		}

		sleep(1);
	}
}

bool IntervalTimer::CheckTimeInterval()
{
	const SimpleTime ST_NOW = SimpleTime::Now();
	int diff_time = m_pfDifferTime(m_sRecordTime, ST_NOW);
	m_sRecordTime = ST_NOW;

	return (diff_time != 0);
}

void IntervalTimer::SendNotification()
{
	if ( !m_sTimers.empty() )
	{
		for ( std::set<TimerInterface*>::iterator it = m_sTimers.begin(); it != m_sTimers.end(); ++it )
		{
			(*it)->Notify();
		}
	}
}

}	// namespace base

