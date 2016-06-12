#pragma once

#include "alarm.h"

struct AnaDBInfo;

// 波动告警
class AlarmFluctuate : public Alarm
{
public:
	AlarmFluctuate();
	virtual ~AlarmFluctuate();

public:
	// 解析告警表达式
	virtual void AnalysisExpression(const std::string& exp) throw(base::Exception);

	// 生成告警事件
	virtual bool GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event);

public:
	// 设置
	void SetDBInfo(AnaDBInfo& db_info);

private:
	AnaDBInfo*	m_pDBInfo;
};

