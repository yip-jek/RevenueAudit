#include "alarm.h"

Alarm::Alarm(int id /*= 0*/)
:m_ID(id)
{
}

Alarm::~Alarm()
{
}

int Alarm::GetAlarmID() const
{
	return m_ID;
}

std::string Alarm::GetRegion() const
{
	return m_region;
}

base::SimpleTime Alarm::GetAlarmTime() const
{
	return m_stAlarmTime;
}

base::SimpleTime Alarm::GetGeneTime() const
{
	return m_stGeneTime;
}

bool Alarm::SetAlarmID(int id)
{
	if ( id > 0 )
	{
		m_ID = id;
		return true;
	}

	return false;
}

bool Alarm::SetRegion(const std::string& region)
{
	if ( region.empty() )
	{
		return false;
	}

	m_region = region;
	return true;
}

void Alarm::SetAlarmTime(const base::SimpleTime& st)
{
	m_stAlarmTime = st;
}

void Alarm::SetGeneTime()
{
	m_stGeneTime = base::SimpleTime::Now();
}

