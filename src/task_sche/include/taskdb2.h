#pragma once

#include "basedb2.h"
#include "struct.h"

class TaskDB2 : public base::BaseDB2
{
public:
	// 数据库错误代码
	enum TASK_DB_ERROR
	{
		TDB_ERR_TAB_EXISTS     = -20000001,				// 检查表存在性失败
		TDB_ERR_SEL_NEW_TREQ   = -20000002,				// 查询新的任务请求失败
		TDB_ERR_SEL_KPI_INFO   = -20000003,				// 查询指标规则信息失败
		TDB_ERR_SEL_TASK_STATE = -20000004,				// 查询任务状态失败
		TDB_ERR_UPD_TASK_REQ   = -20000005,				// 更新任务请求失败
	};

public:
	TaskDB2(const DBInfo& db_info);
	virtual ~TaskDB2();

public:
	// 设置任务请求表
	void SetTabTaskRequest(const std::string& tab_taskreq);

	// 设置指标规则表
	void SetTabKpiRule(const std::string& tab_kpirule);

	// 设置采集规则表
	void SetTabEtlRule(const std::string& tab_etlrule);

	// 表是否存在
	bool IsTableExists(const std::string& tab_name) throw(base::Exception);

	// 查询新的任务请求
	void SelectNewTaskRequest(std::vector<TaskReqInfo>& vec_trinfo) throw(base::Exception);

	// 查询任务状态
	void SelectTaskState(int seq, std::string& task_state) throw(base::Exception);

	// 更新任务请求
	void UpdateTaskRequest(TaskReqInfo& task_req) throw(base::Exception);

	// 查询指标规则信息
	void SelectKpiRule(const std::string& kpi, KpiRuleInfo& kpi_info) throw(base::Exception);

private:
	std::string m_tabTaskReq;				// 任务请求表
	std::string m_tabKpiRule;				// 指标规则表
	std::string m_tabEtlRule;				// 采集规则表
};

