#include "alarmevent.h"


AlarmEvent::AlarmEvent()
:alarmLevel(ALARM_LV_00)
,alarmState(ASTAT_00)
{
}

AlarmEvent::AlarmEvent(const AlarmEvent& ae)
:eventID(ae.eventID)
,eventCont(ae.eventCont)
,eventDesc(ae.eventDesc)
,alarmLevel(ae.alarmLevel)
,alarmID(ae.alarmID)
,alarmTime(ae.alarmTime)
,alarmState(ae.alarmState)
{
}

AlarmEvent::~AlarmEvent()
{
}

const AlarmEvent& AlarmEvent::operator = (const AlarmEvent& ae)
{
	if ( this != &ae )
	{
		this->eventID    = ae.eventID;
		this->eventCont  = ae.eventCont;
		this->eventDesc  = ae.eventDesc;
		this->alarmLevel = ae.alarmLevel;
		this->alarmID    = ae.alarmID;
		this->alarmTime  = ae.alarmTime;
		this->alarmState = ae.alarmState;
	}

	return *this;
}

std::string AlarmEvent::AlarmLevelToString(AlarmEvent::ALARM_LEVEL a_lv)
{
	switch ( a_lv )
	{
	case ALARM_LV_00:		// 未知等级
		return "<UNKNOWN LEVEL>";
	case ALARM_LV_01:		// 一般告警
		return "LV_01";
	case ALARM_LV_02:		// 较严重告警
		return "LV_02";
	case ALARM_LV_03:		// 严重告警
		return "LV_03";
	default:
		return std::string();
	}
}

std::string AlarmEvent::AlarmStateToString(AlarmEvent::ALARM_STATE a_stat)
{
	/*
	   00 – 未知状态
	   01 – 告警生成
	   02 – 告警派单
	   03 – 告警消除
	 */
	switch ( a_stat )
	{
	}
}

