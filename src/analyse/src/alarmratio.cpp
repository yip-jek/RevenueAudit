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

	//throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 目标表的时间字段缺失，无法进行波动告警！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
}

void AlarmRatio::AnalyseTargetData(std::vector<std::vector<std::string> >& vec2_data)
{
	m_pLog->Output("[Alarm] 进行对比告警分析 ...");
}

bool AlarmRatio::GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event)
{
	return false;
}
