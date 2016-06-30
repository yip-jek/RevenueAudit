#include "alarmfluctuate.h"
#include "anataskinfo.h"
#include "anadbinfo.h"
#include "pubstr.h"
#include "pubtime.h"
#include "simpletime.h"
#include "log.h"
#include "thresholdcompare.h"


AlarmFluctuate::AlarmFluctuate()
{
}

AlarmFluctuate::~AlarmFluctuate()
{
}

void AlarmFluctuate::AnalyseExpression() throw(base::Exception)
{
	// 告警表达式格式：[A,B,...]|[mon/day][+/-][月数/日数][大于(>)/大于等于(>=)][比率(百分数/小数)]
	// 例如：A,C,D|day-1>=30%
	const std::string ALARM_EXP = base::PubStr::TrimB(m_pAlarmRule->AlarmExpress);
	m_pLog->Output("[Alarm] 告警表达式：%s", ALARM_EXP.c_str());

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(ALARM_EXP, "|", vec_str);
	if ( vec_str.size() != 2 )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	AnalyseColumns(vec_str[0]);

	AnalyseFluctExp(vec_str[1]);
}

void AlarmFluctuate::AnalyseTargetData(std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception)
{
	m_pLog->Output("[Alarm] 进行波动告警分析 ...");

	// 是否有对比数据？
	if ( !m_mCompareData.empty() )
	{
		std::string key;
		std::map<std::string, std::vector<std::string> >::iterator m_it;

		const size_t VEC2_SIZE = vec2_data.size();
		for ( size_t i = 0; i < VEC2_SIZE; ++i )
		{
			std::vector<std::string>& ref_vec1 = vec2_data[i];

			// 生成维度 key 值
			key = GenerateDimKey(ref_vec1);

			// 已存在于结果数据中，则跳过
			if ( m_mResultData.find(key) != m_mResultData.end() )
			{
				continue;
			}

			// 对比数据中有相同维度的数据
			if ( (m_it = m_mCompareData.find(key)) != m_mCompareData.end() )
			{
				AlarmCalculation(key, ref_vec1, m_it->second);
			}
		}
	}
	else
	{
		m_pLog->Output("<WARNING> [Alarm] 无波动对比数据！");
	}
}

std::string AlarmFluctuate::GetFluctuateDate() const
{
	return m_strAlarmDate;
}

void AlarmFluctuate::SetCompareData(std::vector<std::vector<std::string> >& vec2_data)
{
	std::string key;
	const size_t VEC2_SIZE = vec2_data.size();
	for ( size_t i = 0; i < VEC2_SIZE; ++i )
	{
		std::vector<std::string>& ref_vec1 = vec2_data[i];

		// 生成维度 key 值
		key = GenerateDimKey(ref_vec1);

		m_mCompareData[key] = ref_vec1;
	}

	// 清除数据
	std::vector<std::vector<std::string> >().swap(vec2_data);
}

void AlarmFluctuate::AnalyseColumns(const std::string& exp_columns) throw(base::Exception)
{
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(exp_columns, ",", vec_str);

	const int VEC_SIZE = vec_str.size();
	if ( VEC_SIZE <= 0 )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 告警表达式没有指定列！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		CollectValueColumn(vec_str[i]);
	}
}

void AlarmFluctuate::AnalyseFluctExp(const std::string& exp_fluct) throw(base::Exception)
{
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(exp_fluct, ">", vec_str);
	if ( vec_str.size() != 2 )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", exp_fluct.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	// 先尝试特殊时间格式转换
	if ( !base::PubTime::TheDateOf(vec_str[0], m_strAlarmDate) )
	{
		// （失败后）再尝试以时间表达式转换
		base::PubTime::DATE_TYPE d_type;
		if ( !base::PubTime::DateApartFromNow(vec_str[0], d_type, m_strAlarmDate) )
		{
			throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", exp_fluct.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
		}
	}

	std::string& ref_str = vec_str[1];
	if ( ref_str.empty() )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", exp_fluct.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	// 处理等于号
	bool contain_equal = DealWithEqual(ref_str);

	m_expAlarmThreshold = base::PubStr::TrimB(ref_str);
	if ( !base::PubStr::StrTrans2Double(m_expAlarmThreshold, m_alarmThreshold) )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", exp_fluct.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}
	// 限定告警阈值的最小值（零值）
	if ( m_alarmThreshold < 1e-6 )
	{
		m_pLog->Output("[Alarm] 告警阈值为：%lf，小于极限零值，重置为零值：1e-6", m_alarmThreshold);
		m_alarmThreshold = 1e-6;
	}

	DetermineThresholdCompare(contain_equal);

	if ( m_pTaskInfo->ResultType != AnaTaskInfo::TABTYPE_COMMON )	// 重置目标表名
	{
		m_pDBInfo->target_table = base::PubStr::TrimB(m_pTaskInfo->TableName) + "_" + m_strAlarmDate;
	}
	else if ( !m_pDBInfo->time_stamp )		// 普通表且没有时间字段
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 目标表的时间字段缺失，无法进行波动告警！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}
}

void AlarmFluctuate::AlarmCalculation(const std::string& key, std::vector<std::string>& vec_target, std::vector<std::string>& vec_src) throw(base::Exception)
{
	const int TARGET_SIZE = vec_target.size();
	const int SRC_SIZE    = vec_src.size();

	std::map<std::string, std::vector<AlarmData> >::iterator m_it;
	std::set<int>::iterator s_it;
	double dou_target    = 0.0;
	double dou_src       = 0.0;
	double dou_threshold = 0.0;
	AlarmData a_data;

	for ( s_it = m_setValCol.begin(); s_it != m_setValCol.end(); ++s_it )
	{
		const int& index = *s_it;
		if ( index >= TARGET_SIZE )
		{
			throw base::Exception(AE_ALARM_CALC_FAILED, "[ALARM] 目标数据不存在的列：%d (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", (index+1), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
		}
		if ( index >= SRC_SIZE )
		{
			throw base::Exception(AE_ALARM_CALC_FAILED, "[ALARM] 源数据不存在的列：%d (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", (index+1), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
		}

		std::string& ref_target = vec_target[index];
		if ( !base::PubStr::T1TransT2(ref_target, dou_target) )
		{
			throw base::Exception(AE_ALARM_CALC_FAILED, "[ALARM] 目标数据非精度类型：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ref_target.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
		}

		std::string& ref_src = vec_src[index];
		if ( !base::PubStr::T1TransT2(ref_src, dou_src) )
		{
			throw base::Exception(AE_ALARM_CALC_FAILED, "[ALARM] 源数据非精度类型：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ref_src.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
		}

		// 是否达到告警阈值？
		if ( m_pThresholdCompare->ReachThreshold(dou_target, dou_src, &dou_threshold) )
		{
			// 告警时间为当前时间（精确到秒）
			a_data.alarm_date       = base::SimpleTime::Now().Time14();
			// 告警目标名称为对应的字段名
			a_data.alarm_targetname = m_pDBInfo->vec_fields[index].CN_name;
			a_data.alarm_targetval  = ref_target;
			// 告警对比源名称为波动时间
			a_data.alarm_srcname    = m_strAlarmDate;
			a_data.alarm_srcval     = ref_src;
			// 当前阈值
			a_data.reach_threshold = dou_threshold;

			m_it = m_mResultData.find(key);
			if ( m_it != m_mResultData.end() )		// 已经存在
			{
				m_it->second.push_back(a_data);
			}
			else	// 不存在
			{
				m_mResultData[key].push_back(a_data);
			}
		}
	}
}

bool AlarmFluctuate::DealWithEqual(std::string& exp) throw(base::Exception)
{
	base::PubStr::Trim(exp);

	if ( exp.empty() )
	{
		throw base::Exception(AE_ALARM_CALC_FAILED, "[ALARM] 无法识别的告警表达式：没有配置阈值！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	if ( '=' == exp[0] )	// 包含等号
	{
		// 删除开头的“等于”
		exp.erase(0, 1);
		return true;
	}

	return false;
}

std::string AlarmFluctuate::GetAlarmEventCont()
{
	return ("波动告警_对比" + m_strAlarmDate + "的数据_超过告警阈值_" + m_expAlarmThreshold);
}

std::string AlarmFluctuate::GetAlarmEventDesc(const std::string& key, const AlarmData& a_data)
{
	std::string event_desc = "波动告警：<维度>" + key + "的<值>" + a_data.alarm_targetname + "的数据";
	event_desc += (a_data.alarm_targetval + ", 对比" + a_data.alarm_srcname + "的数据");
	event_desc += (a_data.alarm_srcval + ", 阈值达到");

	std::string str_tmp;
	base::PubStr::T1TransT2(a_data.reach_threshold, str_tmp);
	event_desc += (str_tmp + ", 超过了告警阈值" + m_expAlarmThreshold);
	return event_desc;
}

