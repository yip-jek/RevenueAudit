#include "alarmfluctuate.h"
#include "anataskinfo.h"
#include "anadbinfo.h"
#include "pubstr.h"
#include "pubtime.h"
#include "alarmevent.h"


AlarmFluctuate::AlarmFluctuate()
:m_pDBInfo(NULL)
{
}

AlarmFluctuate::~AlarmFluctuate()
{
}

void AlarmFluctuate::AnalysisExpression(const std::string& exp) throw(base::Exception)
{
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(ALARM_EXP, "|", vec_str);

	if ( vec_str.size() != 2 )
	{
		throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
	}

	const int KPI_DIM_SIZE = info.vecKpiDimCol.size() - (m_dbinfo.time_stamp?1:0);		// 去除时间列
	const int KPI_VAL_SIZE = info.vecKpiValCol.size();

	const std::string EXP_FIRST_PART  = vec_str[0];
	const std::string EXP_SECOND_PART = vec_str[1];

	base::PubStr::Str2StrVector(EXP_FIRST_PART, ",", vec_str);
	int v_size = vec_str.size();
	if ( v_size <= 0 )
	{
		throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "告警表达式没有指定列！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
	}

	std::set<int> set_index;
	for ( int i = 0; i < v_size; ++i )
	{
		std::string& ref_str = vec_str[i];
		if ( ref_str.size() != 1 )		// 无效
		{
			throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
		}

		base::PubStr::Trim(ref_str);
		int index = ref_str[0] - 'A';
		if ( index < 0 || index >= KPI_VAL_SIZE )	// 不在有效范围
		{
			throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "告警表达式中存在不在有效范围的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ref_str.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
		}

		// 计算“值”所处的列
		index += KPI_DIM_SIZE;
		if ( set_index.find(index) != set_index.end() )
		{
			throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "告警表达式中存在重复的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ref_str.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
		}

		set_index.insert(index);
	}

	base::PubStr::Str2StrVector(EXP_SECOND_PART, ">", vec_str);
	if ( vec_str.size() != 2 )
	{
		throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
	}

	base::PubTime::DATE_TYPE d_type;
	std::string str_date;
	if ( !base::PubTime::DateApartFromNow(vec_str[0], d_type, str_date) )
	{
		throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
	}

	std::string& ref_r = vec_str[1];
	if ( ref_r.empty() )
	{
		throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
	}

	// 是否只是“大于”
	bool only_greater = true;
	if ( '=' == ref_r[0] )		// 大于等于
	{
		only_greater = false;

		// 删除开头的“等于”
		ref_r.erase(0, 1);
	}

	double d_rat = 0.0;
	if ( !base::PubStr::StrTrans2Double(ref_r, d_rat) )
	{
		throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
	}

	if ( info.ResultType != AnaTaskInfo::TABTYPE_COMMON )	// 重置目标表名
	{
		m_dbinfo.target_table = base::PubStr::TrimB(info.TableName) + "_" + str_date;
	}
	else if ( !m_dbinfo.time_stamp )
	{
		throw base::Exception(ANAERR_FLUCTUATE_ALARM_FAILED, "目标表的时间字段缺失，无法进行波动告警！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), info.KpiID.c_str(), alarm_rule.AlarmID.c_str(), __FILE__, __LINE__);
	}


}

bool AlarmFluctuate::GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event)
{
	return false;
}

void AlarmFluctuate::SetDBInfo(AnaDBInfo& db_info)
{
	m_pDBInfo = &db_info;
}

