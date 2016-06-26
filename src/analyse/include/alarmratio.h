#pragma once

#include "alarm.h"

// 对比告警
class AlarmRatio : public Alarm
{
public:
	AlarmRatio();
	virtual ~AlarmRatio();

public:
	// 解析告警表达式
	virtual void AnalyseExpression() throw(base::Exception);

	// 分析目标数据
	virtual void AnalyseTargetData(std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception);

	// 告警事件内容
	virtual std::string GetAlarmEventCont();

	// 告警事件描述
	virtual std::string GetAlarmEventDesc(const std::string& key, const AlarmData& a_data);

private:
	// 解析列组
	void AnalyseColumns(const std::string& exp_columns) throw(base::Exception);

	// 解析表达式比率
	void AnalyseRatioExp(const std::string& exp_ratio) throw(base::Exception);

	// 告警计算，并记录达到阈值的数据
	void AlarmCalculation(const std::string& key, std::vector<std::string>& vec_str) throw(base::Exception);

private:
	std::string	m_expAlarmThreshold;	// 告警阈值（取自告警表达式）
};

