#pragma once

#include <vector>
#include "basedb2.h"

namespace base
{

class Exception;

}

struct DBInfo;
struct YDAlarmReq;

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

public:
	// 获取告警请求
	void SelectAlarmRequest(std::vector<YDAlarmReq>& vecAlarmReq) throw(base::Exception);

	// 更新告警请求
	void UpdateAlarmRequest(const YDAlarmReq& alarm_req) throw(base::Exception);

private:
	std::string m_tabAlarmRequest;				// 告警请求表
	std::string m_tabAlarmThreshold;			// 告警阈值表
	std::string m_tabAlarmInfo;					// 告警信息表
};

