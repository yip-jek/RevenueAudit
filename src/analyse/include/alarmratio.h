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
	virtual void AnalysisExpression(const std::string& exp) throw(base::Exception);

	// 生成告警事件
	virtual bool GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event);
};

