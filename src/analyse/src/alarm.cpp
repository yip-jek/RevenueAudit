#include "alarm.h"
#include "anataskinfo.h"
#include "anadbinfo.h"
#include "log.h"
#include "pubstr.h"
#include "thresholdcompare.h"
#include "alarmevent.h"
#include "uniformcodetransfer.h"


Alarm::Alarm()
:m_pLog(base::Log::Instance())
,m_pTaskInfo(NULL)
,m_pDBInfo(NULL)
,m_pAlarmRule(NULL)
,m_pUnicodeTransfer(NULL)
,m_alarmThreshold(0.0)
,m_pThresholdCompare(NULL)
,m_kpiDimSize(0)
,m_kpiValSize(0)
,m_dimRegionIndex(0)
,m_dimChannelIndex(0)
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

bool Alarm::GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event)
{
	std::vector<AlarmEvent> v_e;
	if ( !m_mResultData.empty() )
	{
		m_pLog->Output("[Alarm] 生成告警事件 ...");

		AlarmEvent a_event;
		a_event.eventCont = GetAlarmEventCont();
		a_event.alarmLevel = AlarmEvent::ALARM_LV_01;
		a_event.alarmID    = m_pAlarmRule->AlarmID;
		a_event.alarmState = AlarmEvent::ASTAT_01;

		std::map<std::string, std::vector<AlarmData> >::iterator m_it = m_mResultData.begin();
		for ( ; m_it != m_mResultData.end(); ++m_it )
		{
			const std::string& REF_KEY      = m_it->first;
			std::vector<AlarmData>& ref_vec = m_it->second;

			const int VEC_AD_SIZE = ref_vec.size();
			for ( int i = 0; i < VEC_AD_SIZE; ++i )
			{
				AlarmData& ref_ad = ref_vec[i];

				//a_event.eventID = 0;		// 不设定告警事件 ID
				a_event.eventDesc = GetAlarmEventDesc(REF_KEY, ref_ad);
				a_event.alarmTime = ref_ad.alarm_date;

				v_e.push_back(a_event);
			}
		}

		m_pLog->Output("[Alarm] 生成的告警事件数：%llu", v_e.size());

		v_e.swap(vec_event);
		return true;
	}
	else
	{
		v_e.swap(vec_event);

		m_pLog->Output("[Alarm] 无告警事件.");
		return false;
	}
}

void Alarm::SetTaskDBInfo(AnaTaskInfo& t_info, AnaDBInfo& db_info)
{
	m_pTaskInfo = &t_info;
	m_pDBInfo   = &db_info;

	m_kpiDimSize = m_pTaskInfo->vecKpiDimCol.size();
	m_kpiValSize = m_pTaskInfo->vecKpiValCol.size();

	// 设置地市和渠道维度字段的位置索引
	m_dimRegionIndex  = m_pTaskInfo->GetDimEWTypeIndex(KpiColumn::EWTYPE_REGION);
	m_dimChannelIndex = m_pTaskInfo->GetDimEWTypeIndex(KpiColumn::EWTYPE_CHANNEL);
}

void Alarm::SetAlarmRule(AlarmRule& alarm_rule)
{
	m_pAlarmRule = &alarm_rule;
}

void Alarm::SetUnicodeTransfer(UniformCodeTransfer& unicode_transfer)
{
	m_pUnicodeTransfer = &unicode_transfer;
}

std::string Alarm::GenerateDimKey(std::vector<std::string>& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < (size_t)m_kpiDimSize )
	{
		throw base::Exception(ANAERR_ALARM_GENERATE_DIMKEY, "[ALARM] 无法生成维度 KEY 值：数据列数(size:%lu) < 维度列数(size:%d) (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", vec_str.size(), m_kpiDimSize, m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	std::string dim;
	std::string dim_key;
	for ( int i = 0; i < m_kpiDimSize; ++i )
	{
		// 维度用双单引号引用
		dim = base::PubStr::TrimB(vec_str[i]);
		dim = "'" + TryGetUnicodeCN(dim, i) + "'";

		// 维度 KEY 值：中间用竖线（'|'）分隔
		if ( i != 0 )
		{
			dim_key += ("|" + dim);
		}
		else
		{
			dim_key += dim;
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
		throw base::Exception(ANAERR_ALARM_COLLECT_VALCOL, "[ALARM] 无法识别的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", str_vc.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	int index = str_vc[0] - 'A';

	// 不在有效范围
	if ( index < 0 || index >= m_kpiValSize )
	{
		throw base::Exception(ANAERR_ALARM_COLLECT_VALCOL, "[ALARM] 不在有效范围的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", str_vc.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	// 计算“值”所处的列
	index += m_kpiDimSize;

	// 列重复
	if ( m_setValCol.find(index) != m_setValCol.end() )
	{
		throw base::Exception(ANAERR_ALARM_COLLECT_VALCOL, "[ALARM] 存在重复的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", str_vc.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
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

std::string Alarm::TryGetUnicodeCN(const std::string& unicode, const int& index)
{
	if ( m_pUnicodeTransfer != NULL )	// 使用统一编码转换
	{
		if ( index == m_dimRegionIndex )	// 地市维度
		{
			return m_pUnicodeTransfer->TryGetRegionCNName(unicode);
		}
		else if ( index == m_dimChannelIndex )	// 渠道维度
		{
			return m_pUnicodeTransfer->TryGetChannelCNName(unicode);
		}
		else
		{
			return unicode;
		}
	}
	else	// 无法转换
	{
		return unicode;
	}
}

