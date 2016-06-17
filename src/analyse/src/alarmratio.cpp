#include "alarmratio.h"
#include "anataskinfo.h"
#include "pubstr.h"
#include "pubtime.h"
#include "log.h"
#include "alarmevent.h"


AlarmRatio::AlarmRatio()
{
}

AlarmRatio::~AlarmRatio()
{
}

void AlarmRatio::AnalyseExpression() throw(base::Exception)
{
	// 告警表达式格式：[A/B/C/...]-[A/B/C/...][大于(>)/大于等于(>=)][比率(百分数/小数)]
	// 例如：A-B > 0.3
	const std::string ALARM_EXP = base::PubStr::TrimB(m_pAlarmRule->AlarmExpress);
	m_pLog->Output("[Alarm] 告警表达式：%s", ALARM_EXP.c_str());
}

void AlarmRatio::AnalyseTargetData(std::vector<std::vector<std::string> >& vec2_data)
{
	m_pLog->Output("[Alarm] 进行对比告警分析 ...");
}

bool AlarmRatio::GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event)
{
	return false;
}

