#include "alarmratio.h"
#include "anataskinfo.h"
#include "anadbinfo.h"
#include "pubstr.h"
#include "pubtime.h"
#include "simpletime.h"
#include "log.h"
#include "thresholdcompare.h"


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

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(ALARM_EXP, "|", vec_str);
	if ( vec_str.size() != 2 )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ALARM_EXP.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	AnalyseColumns(vec_str[0]);

	AnalyseRatioExp(vec_str[1]);
}

void AlarmRatio::AnalyseTargetData(std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception)
{
	m_pLog->Output("[Alarm] 进行对比告警分析 ...");

	if ( m_setValCol.size() != 2 )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 告警规则所指定的列数不正确：%lu (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", m_setValCol.size(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	std::string key;
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

		AlarmCalculation(key, ref_vec1);
	}
}

std::string AlarmRatio::GetAlarmEventCont()
{
	std::string event_cont = "对比告警_对比" + base::PubStr::Int2Str((*m_setValCol.begin())+1) + "和";
	event_cont += (base::PubStr::Int2Str((*m_setValCol.rbegin())+1) + "两组数据_超过告警阈值_" + m_expAlarmThreshold);
	return event_cont;
}

std::string AlarmRatio::GetAlarmEventDesc(const std::string& key, const AlarmData& a_data)
{
	std::string event_desc = "对比告警：<维度>" + key + "第" + base::PubStr::Int2Str((*m_setValCol.begin())+1) + "列的数据" + a_data.alarm_targetval;
	event_desc += (", 对比第" + base::PubStr::Int2Str((*m_setValCol.rbegin())+1) + "列的数据" + a_data.alarm_srcval + ", 阈值达到");
	event_desc += (base::PubStr::Double2Str(a_data.reach_threshold) + ", 超过了告警阈值" + m_expAlarmThreshold);
	return event_desc;
}

void AlarmRatio::AnalyseColumns(const std::string& exp_columns) throw(base::Exception)
{
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(exp_columns, "-", vec_str);

	// 只支持两组数据的对比
	if ( vec_str.size() != 2 )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 无法识别的告警规则表达式所指定的列：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", exp_columns.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	CollectValueColumn(vec_str[0]);
	CollectValueColumn(vec_str[1]);
}

void AlarmRatio::AnalyseRatioExp(const std::string& exp_ratio) throw(base::Exception)
{
	std::string exp = base::PubStr::TrimB(exp_ratio);
	if ( exp.empty() )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 无法识别的告警表达式：没有配置阈值！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	// 是否包含等号 ('=')
	bool contain_equal = ('=' == exp[0]);
	if ( contain_equal )
	{
		// 删除开头的“等于”
		exp.erase(0, 1);
	}

	m_expAlarmThreshold = base::PubStr::TrimB(exp);
	if ( !base::PubStr::StrTrans2Double(m_expAlarmThreshold, m_alarmThreshold) )
	{
		throw base::Exception(AE_ANALYSIS_EXP_FAILED, "[ALARM] 无法识别的告警规则表达式：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", exp.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}
	// 限定告警阈值的最小值（零值）
	if ( m_alarmThreshold < 1e-6 )
	{
		m_pLog->Output("[Alarm] 告警阈值为：%lf，小于极限零值，重置为零值：1e-6", m_alarmThreshold);
		m_alarmThreshold = 1e-6;
	}

	DetermineThresholdCompare(contain_equal);
}

void AlarmRatio::AlarmCalculation(const std::string& key, std::vector<std::string>& vec_str) throw(base::Exception)
{
	std::map<std::string, std::vector<AlarmData> >::iterator m_it;
	double dou_target    = 0.0;
	double dou_src       = 0.0;
	double dou_threshold = 0.0;
	AlarmData a_data;

	const int TARGET_INDEX  = *m_setValCol.begin();
	std::string& ref_target = vec_str[TARGET_INDEX];
	if ( !base::PubStr::Str2Double(ref_target, dou_target) )
	{
		throw base::Exception(AE_ALARM_CALC_FAILED, "[ALARM] 目标数据非精度类型：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ref_target.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	const int SRC_INDEX  = *m_setValCol.rbegin();
	std::string& ref_src = vec_str[SRC_INDEX];
	if ( !base::PubStr::Str2Double(ref_src, dou_src) )
	{
		throw base::Exception(AE_ALARM_CALC_FAILED, "[ALARM] 源数据非精度类型：%s (KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", ref_src.c_str(), m_pTaskInfo->KpiID.c_str(), m_pAlarmRule->AlarmID.c_str(), __FILE__, __LINE__);
	}

	// 是否达到告警阈值？
	if ( m_pThresholdCompare->ReachThreshold(dou_target, dou_src, &dou_threshold) )
	{
		// 告警时间为当前时间（精确到秒）
		a_data.alarm_date       = base::SimpleTime::Now().Time14();
		// 告警目标名称为对应的字段名
		a_data.alarm_targetname = m_pDBInfo->GetAnaField(TARGET_INDEX).CN_name;
		a_data.alarm_targetval  = ref_target;
		// 告警源名称为对应的字段名
		a_data.alarm_srcname    = m_pDBInfo->GetAnaField(SRC_INDEX).CN_name;
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

