#include "alarmratio.h"
#include "pubstr.h"
#include "pubtime.h"
#include "alarmevent.h"


AlarmRatio::AlarmRatio()
{
}

AlarmRatio::~AlarmRatio()
{
}

void AlarmRatio::AnalysisExpression(const std::string& exp) throw(base::Exception)
{
}

bool AlarmRatio::GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event)
{
	return false;
}

