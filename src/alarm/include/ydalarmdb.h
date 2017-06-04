#pragma once

#include <map>
#include <vector>
#include "basedb2.h"

struct DBInfo;
struct YDAlarmReq;
struct YDAlarmThreshold;
struct YDAlarmData;
struct YDAlarmInfo;

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

	// 设置数据源表
	void SetTabSrcData(const std::string& tab_srcdata);

public:
	// 获取告警请求
	void SelectAlarmRequest(std::map<int, YDAlarmReq>& mapAlarmReq) throw(base::Exception);

	// 更新告警请求
	void UpdateAlarmRequest(const YDAlarmReq& alarm_req) throw(base::Exception);

	// 获取告警阈值
	void SelectAlarmThreshold(std::vector<YDAlarmThreshold>& vecAlarmThres) throw(base::Exception);

	// 采集告警数据
	void SelectAlarmData(int seq, const std::string& condition, std::vector<YDAlarmData>& vecData) throw(base::Exception);

	// 存储告警信息
	void InsertAlarmInfo(const YDAlarmInfo& alarm_info) throw(base::Exception);

private:
	std::string m_tabAlarmRequest;				// 告警请求表
	std::string m_tabAlarmThreshold;			// 告警阈值表
	std::string m_tabAlarmInfo;					// 告警信息表
	std::string m_tabSrcData;					// 数据源表
};

