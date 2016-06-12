#pragma once

#include <string>
#include <vector>
#include "exception.h"

struct AnaTaskInfo;
class AlarmEvent;

// （基础）告警类
class Alarm
{
public:
	Alarm();
	virtual ~Alarm();

public:
	// 设置
	virtual void SetTaskInfo(AnaTaskInfo& t_info);

	// 解析告警表达式
	virtual void AnalysisExpression(const std::string& exp) throw(base::Exception) = 0;

	// 生成告警事件
	virtual bool GenerateAlarmEvent(std::vector<AlarmEvent>& vec_event) = 0;

protected:
	AnaTaskInfo*	m_pTaskInfo;
};

