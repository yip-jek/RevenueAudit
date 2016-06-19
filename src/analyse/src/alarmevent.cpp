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

