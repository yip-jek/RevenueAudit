#include "alarm.h"
#include "anataskinfo.h"


Alarm::Alarm()
:m_pTaskInfo(NULL)
{
}

Alarm::~Alarm()
{
}

void Alarm::SetTaskInfo(AnaTaskInfo& t_info)
{
	m_pTaskInfo = &t_info;
}

