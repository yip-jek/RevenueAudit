#pragma once

#include "alarm.h"

struct AlarmData;

// 波动告警
class AlarmFluctuate : public Alarm
{
public:
	AlarmFluctuate();
	virtual ~AlarmFluctuate();

public:
	// 解析告警表达式
	virtual void AnalyseExpression() throw(base::Exception);

	// 分析目标数据
	virtual void AnalyseTargetData(std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception);

public:
	// 获取告警波动时间
	std::string GetFluctuateDate() const;

	// 设置告警对比数据
	void SetCompareData(std::vector<std::vector<std::string> >& vec2_data);

	// 告警事件内容
	virtual std::string GetAlarmEventCont();

	// 告警事件描述
	virtual std::string GetAlarmEventDesc(const std::string& key, const AlarmData& a_data);

private:
	// 解析列组
	void AnalyseColumns(const std::string& exp_columns) throw(base::Exception);

	// 解析波动表达式
	void AnalyseFluctExp(const std::string& exp_fluct) throw(base::Exception);

	// 告警计算，并记录达到阈值的数据
	void AlarmCalculation(const std::string& key, std::vector<std::string>& vec_target, std::vector<std::string>& vec_src) throw(base::Exception);

	// 处理表达式中的等号
	// 返回：true-包含等号，false-不包含等号
	bool DealWithEqual(std::string& exp) throw(base::Exception);

private:
	std::string											m_strAlarmDate;			// 告警波动时间
	std::string											m_expAlarmThreshold;	// 告警阈值（取自告警表达式）
	std::map<std::string, std::vector<std::string> >	m_mCompareData;			// 告警对比数据
};

