#pragma once

#include "canadb2.h"

// 一点稽核-数据库类
class YDAnaDB2 : public CAnaDB2
{
public:
	YDAnaDB2(const std::string& db_name, const std::string& user, const std::string& pwd);
	virtual ~YDAnaDB2();

public:
	// 设置任务日程日志表
	void SetTabTaskScheLog(const std::string& t_tslog);

	// 设置告警请求表
	void SetTabAlarmRequest(const std::string& t_alarmreq);

	// 更新任务日程日志表状态
	void UpdateTaskScheLogState(int log, const std::string& end_time, const std::string& state, const std::string& state_desc, const std::string& remark);

	// 获取全量地市（唯一）
	void SelectAllCity(std::vector<std::string>& vec_city);

protected:
	std::string m_tabTaskScheLog;		// 任务日程日志表
	std::string m_tabAlarmRequest;		// 告警请求表
};

