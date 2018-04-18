#include "sectimer.h"
#include <unistd.h>
#include "simpletime.h"

SecTimer::SecTimer(long sec)
:m_sectime(0)
,m_curtime(0)
{
	Set(sec);
}

SecTimer::~SecTimer()
{
}

bool SecTimer::Set(long sec)
{
	if ( sec > 0 )
	{
		m_sectime = sec;
		return true;
	}

	return false;
}

bool SecTimer::Start()
{
	if ( m_sectime > 0 )
	{
		m_curtime = base::SimpleTime::CurrentTime();
		return true;
	}

	return false;
}

bool SecTimer::IsTimeUp()
{
	long t_now = base::SimpleTime::CurrentTime();
	if ( t_now - m_curtime >= m_sectime )
	{
		m_curtime = t_now;
		return true;
	}

	return false;
}

void SecTimer::WaitForTimeUp()
{
	long t_val = m_curtime + m_sectime - base::SimpleTime::CurrentTime();
	if ( t_val > 0 )
	{
		sleep(t_val);
	}

	m_curtime = base::SimpleTime::CurrentTime();
}

