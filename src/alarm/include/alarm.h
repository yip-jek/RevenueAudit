#pragma once

#include "simpletime.h"

class Alarm
{
public:
	explicit Alarm(int id = 0);
	virtual ~Alarm();

public:
	// 获取告警ID
	virtual int GetAlarmID() const;
	// 获取地市
	virtual std::string GetRegion() const;
	// 获取告警时间
	virtual base::SimpleTime GetAlarmTime() const;
	// 获取告警生成时间
	virtual base::SimpleTime GetGeneTime() const;

	// 设置告警ID
	virtual bool SetAlarmID(int id);
	// 设置地市
	virtual bool SetRegion(const std::string& region);
	// 设置告警时间
	virtual void SetAlarmTime(const base::SimpleTime& st);
	// 设置告警生成时间
	virtual void SetGeneTime();

	// 生成告警信息
	virtual std::string GenerateAlarmInfo() = 0;

protected:
	int              m_ID;					// 告警ID
	std::string      m_region;				// 地市
	base::SimpleTime m_stAlarmTime;			// 告警时间
	base::SimpleTime m_stGeneTime;			// 告警生成时间
};

