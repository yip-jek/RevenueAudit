#include "alarm.h"
#include "anataskinfo.h"
#include "anadbinfo.h"
#include "log.h"
#include "pubstr.h"
#include "thresholdcompare.h"


Alarm::Alarm()
:m_pLog(base::Log::Instance())
,m_pTaskInfo(NULL)
,m_pDBInfo(NULL)
,m_pAlarmRule(NULL)
,m_alarmThreshold(0.0)
,m_pThresholdCompare(NULL)
,m_kpiDimSize(0)
,m_kpiValSize(0)
{
}

Alarm::~Alarm()
{
	if ( m_pThresholdCompare != NULL )
	{
		delete m_pThresholdCompare;
		m_pThresholdCompare = NULL;
	}

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

std::string Alarm::GenerateDimKey(std::vector<std::string>& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < m_kpiDimSize )
	{
		throw base::Exception(AE_GENERATE_DIM_KEY_FAILED, "[ALARM] 无法生成维度 KEY 值：数据列数(size:%lu) < 维度列数(size:%d) (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", vec_str.size(), m_kpiDimSize, m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	std::string dim_key;
	for ( int i = 0; i < m_kpiDimSize; ++i )
	{
		// 维度 KEY 值：中间用竖线（'|'）分隔
		if ( i != 0 )
		{
			dim_key += ("|" + base::PubStr::TrimB(vec_str[i]));
		}
		else
		{
			dim_key += base::PubStr::TrimB(vec_str[i]);
		}
	}

	return dim_key;
}

void Alarm::CollectValueColumn(const std::string& val_col) throw(base::Exception)
{
	const std::string str_vc = base::PubStr::TrimUpperB(val_col);

	// 只支持单个字母表示
	if ( str_vc.size() != 1 )
	{
		throw base::Exception(AE_COLLECT_VALCOL_FAILED, "[ALARM] 无法识别的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", str_vc.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	int index = str_vc[0] - 'A';

	// 不在有效范围
	if ( index < 0 || index >= m_kpiValSize )
	{
		throw base::Exception(AE_COLLECT_VALCOL_FAILED, "[ALARM] 不在有效范围的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", str_vc.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	// 计算“值”所处的列
	index += m_kpiDimSize;

	// 列重复
	if ( m_setValCol.find(index) != m_setValCol.end() )
	{
		throw base::Exception(AE_COLLECT_VALCOL_FAILED, "[ALARM] 存在重复的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", str_vc.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	m_setValCol.insert(index);
}

void Alarm::DetermineThresholdCompare(bool contain_equal)
{
	// 是否有资源需要释放？
	if ( m_pThresholdCompare != NULL )
	{
		delete m_pThresholdCompare;
		m_pThresholdCompare = NULL;
	}

	if ( contain_equal )		// 大于或等于
	{
		m_pThresholdCompare = new GE_ThresholdCompare(m_alarmThreshold);
	}
	else		// 大于
	{
		m_pThresholdCompare = new G_ThresholdCompare(m_alarmThreshold);
	}
}

