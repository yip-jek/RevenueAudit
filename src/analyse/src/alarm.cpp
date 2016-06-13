#include "alarm.h"
#include "anataskinfo.h"
#include "anadbinfo.h"
#include "log.h"
#include "pubstr.h"


Alarm::Alarm()
:m_pLog(base::Log::Instance())
,m_pTaskInfo(NULL)
,m_pDBInfo(NULL)
,m_pAlarmRule(NULL)
,m_containEqual(false)
,m_alarmThreshold(0.0)
,m_kpiDimSize(0)
,m_kpiValSize(0)
{
}

Alarm::~Alarm()
{
	base::Log::Release();
}

void Alarm::SetTaskDBInfo(AnaTaskInfo& t_info, AnaDBInfo& db_info)
{
	m_pTaskInfo = &t_info;
	m_pDBInfo   = &db_info;

	m_kpiDimSize = m_pTaskInfo->vecKpiDimCol.size();
	m_kpiValSize = m_pTaskInfo->vecKpiValCol.size();

	// 去除时间列
	if ( m_pDBInfo->time_stamp )
	{
		m_kpiDimSize -= 1;
	}
}

void Alarm::SetAlarmRule(AlarmRule& alarm_rule)
{
	m_pAlarmRule = &alarm_rule;
}

void Alarm::CollectValueColumn(const std::string& val_col) throw(base::Exception)
{
	const std::string str_vc = base::PubStr::TrimUpperB(val_col);

	// 只支持单个字母表示
	if ( str_vc.size() != 1 )
	{
		throw base::Exception(AE_COLLECT_VALCOL_FAILED, "无法识别的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", str_vc.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	int index = str_vc[0] - 'A';

	// 不在有效范围
	if ( index < 0 || index >= m_kpiValSize )
	{
		throw base::Exception(AE_COLLECT_VALCOL_FAILED, "不在有效范围的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", str_vc.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	// 计算“值”所处的列
	index += m_kpiDimSize;

	// 列重复
	if ( m_setValCol.find(index) != m_setValCol.end() )
	{
		throw base::Exception(AE_COLLECT_VALCOL_FAILED, "存在重复的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", str_vc.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	m_setValCol.insert(index);
}

bool Alarm::IsContainEqual(const std::string& exp, size_t* pos /*= NULL*/)
{
	const std::string str_exp = base::PubStr::TrimB(exp);

	size_t p = 0;
	if ( !str_exp.empty() && (p = str_exp.find("=")) != std::string::npos )
	{
		m_containEqual = true;

		if ( pos != NULL )
		{
			*pos = p;
		}
	}
	else
	{
		m_containEqual = false;
	}

	return m_containEqual;
}

bool Alarm::CalculateThreshold(const std::string& left, const std::string& right, double& threshold)
{
	return false;
}

