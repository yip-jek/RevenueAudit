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
	virtual void AnalyseTargetData(std::vector<std::vector<std::string> >& vec2_data);

	// 生成告警事件
	virtual bool GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event);
};

