#pragma once

#include "basedb2.h"

struct DBInfo;

// 一点稽核数据库类
class YDAlarmDB : public base::BaseDB2
{
public:
	YDAlarmDB(const DBInfo& db_info);
	virtual ~YDAlarmDB();

public:
	// 设置告警请求表
	void SetTabAlarmRequest(const std::string& tab_alarmreq);

	// 设置告警阈值表
	void SetTabAlarmThreshold(const std::string& tab_alarmthreshold);

	// 设置告警信息表
	void SetTabAlarmInfo(const std::string& tab_alarminfo);

private:
	std::string m_tabAlarmRequest;				// 告警请求表
	std::string m_tabAlarmThreshold;			// 告警阈值表
	std::string m_tabAlarmInfo;					// 告警信息表
};

