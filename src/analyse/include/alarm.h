#pragma once

#include <string>
#include <vector>
#include <set>
#include <map>
#include "exception.h"

namespace base
{

class Log;

}

struct AnaTaskInfo;
struct AnaDBInfo;
struct AlarmRule;
class AlarmEvent;
class ThresholdCompare;

// 告警数据
struct AlarmData
{
public:
	AlarmData(): reach_threshold(0.0)
	{}

public:
	std::string	alarm_date;				// 告警产生时间
	std::string	alarm_targetname;		// 告警目标名称
	std::string alarm_targetval;		// 告警目标值
	std::string	alarm_srcname;			// 告警比对源名称
	std::string	alarm_srcval;			// 告警比对源值
	double		reach_threshold;		// 当前（达到）的阈值
};

// （基础）告警类
class Alarm
{
public:
	// 错误码
	enum AlarmError
	{
		AE_ANALYSIS_EXP_FAILED     = -3004001,		// 告警表达式解析失败
		AE_COLLECT_VALCOL_FAILED   = -3004002,		// 收集值列失败
		AE_ALARM_CALC_FAILED       = -3004003,		// 告警计算失败
		AE_GENERATE_DIM_KEY_FAILED = -3004004,		// 生成维度 KEY 值失败
	};

public:
	Alarm();
	virtual ~Alarm();

public:
	// 解析告警表达式
	virtual void AnalyseExpression() throw(base::Exception) = 0;

	// 分析目标数据
	virtual void AnalyseTargetData(std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception) = 0;

	// 告警事件内容
	virtual std::string GetAlarmEventCont() = 0;

	// 告警事件描述
	virtual std::string GetAlarmEventDesc(const std::string& key, const AlarmData& a_data) = 0;

public:
	// 设置
	virtual void SetTaskDBInfo(AnaTaskInfo& t_info, AnaDBInfo& db_info);
	virtual void SetAlarmRule(AlarmRule& alarm_rule);

	// 生成告警事件
	virtual bool GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event);

protected:
	// 生成维度 key 值
	virtual std::string GenerateDimKey(std::vector<std::string>& vec_str) throw(base::Exception);

	// 收集值列
	virtual void CollectValueColumn(const std::string& val_col) throw(base::Exception);

	// 确定比较的方法
	virtual void DetermineThresholdCompare(bool contain_equal);

protected:
	base::Log*		m_pLog;

protected:
	AnaTaskInfo*	m_pTaskInfo;
	AnaDBInfo*		m_pDBInfo;
	AlarmRule*		m_pAlarmRule;

protected:
	std::set<int>		m_setValCol;				// 值列集：值的列序号的集合
	double				m_alarmThreshold;			// 告警阈值
	ThresholdCompare*	m_pThresholdCompare;

protected:
	std::map<std::string, std::vector<AlarmData> >	m_mResultData;			// 告警结果数据

protected:
	int m_kpiDimSize;			// 指标维度字段数
	int m_kpiValSize;			// 指标值字段数
};

