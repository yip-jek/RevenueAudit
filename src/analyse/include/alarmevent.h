#pragma once

#include <string>

// 告警事件
class AlarmEvent
{
public:
	AlarmEvent();
	AlarmEvent(const AlarmEvent& ae);
	virtual ~AlarmEvent();

	const AlarmEvent& operator = (const AlarmEvent& ae);

public:
	// 告警等级
	enum ALARM_LEVEL
	{
		ALARM_LV_00		= 0,		// 未知等级
		ALARM_LV_01		= 1,		// 一般告警
		ALARM_LV_02		= 2,		// 较严重告警
		ALARM_LV_03		= 3,		// 严重告警
	};

	// 告警状态
	enum ALARM_STATE
	{
		ASTAT_00	= 0,			// 未知状态
		ASTAT_01	= 1,			// 告警生成
		ASTAT_02	= 2,			// 告警派单
		ASTAT_03	= 3,			// 告警消除
	};

public:
	// 告警等级转换
	static std::string TransAlarmLevel(ALARM_LEVEL a_lv);

	// 告警状态转换
	static int TransAlarmState(ALARM_STATE a_stat);

public:
	int			eventID;			// 告警事件 ID
	std::string	eventCont;			// 告警事件内容
	std::string	eventDesc;			// 告警事件描述
	ALARM_LEVEL	alarmLevel;			// 告警等级
	std::string	alarmID;			// 告警规则 ID
	std::string alarmTime;			// 告警时间，格式：YYYYMMDDHHmiss
	ALARM_STATE	alarmState;			// 告警状态
};

