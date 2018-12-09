#pragma once

#include "basedb2.h"
#include "ycstruct.h"

class YCTaskDB2 : public base::BaseDB2
{
public:
	// 数据库错误代码
	enum YCTASK_DB_ERROR
	{
		TDB_ERR_TAB_EXISTS     = -20000001,				// 检查表存在性失败
		TDB_ERR_SEL_NEW_TREQ   = -20000002,				// 查询新的任务请求失败
		TDB_ERR_SEL_KPI_INFO   = -20000003,				// 查询指标规则信息失败
		TDB_ERR_SEL_TASK_STATE = -20000004,				// 查询任务状态失败
		TDB_ERR_UPD_TASK_REQ   = -20000005,				// 更新任务请求失败
		TDB_ERR_UPD_ETL_TIME   = -20000006,				// 更新采集时间失败
	};

public:
	YCTaskDB2(const DBInfo& db_info);
	virtual ~YCTaskDB2();

public:
	// 设置任务请求表
	void SetTabTaskRequest(const std::string& tab_taskreq);

	// 设置指标规则表
	void SetTabKpiRule(const std::string& tab_kpirule);

	// 设置采集规则表
	void SetTabEtlRule(const std::string& tab_etlrule);

    //设置详情表提交状态表
    void SetTabYLStatus(const std::string& tab_ylStatus);

    //设置报表指标关联表
    void SetTabReportKpiRela(const std::string &tab_reportkpirela);

	// 表是否存在
	bool IsTableExists(const std::string& tab_name) throw(base::Exception);

	// 查询新的任务请求
	void SelectNewTaskRequest(std::vector<TaskReqInfo>& vec_trinfo) throw(base::Exception);

	// 查询任务状态
	void SelectTaskState(TaskState& t_state) throw(base::Exception);

	// 更新任务请求
	void UpdateTaskRequest(TaskReqInfo& task_req) throw(base::Exception);

	// 查询指标规则信息
	void SelectKpiRule(const std::string& kpi, KpiRuleInfo& kpi_info) throw(base::Exception);

	// 更新采集时间
	void UpdateEtlTime(const std::string& etl_id, const std::string& etl_time) throw(base::Exception);

    // 统计21个地市详情表状态为已提交个数
    int CountAllSubmitStatu(TaskReqInfo& ref_taskreq) throw(base::Exception);

    void SelectUndoneTaskRequest(std::vector<TaskReqInfo>& vec_Undone,const std::string& FinalStage) throw(base::Exception);

private:
	std::string m_tabTaskReq;				// 任务请求表
	std::string m_tabKpiRule;				// 指标规则表
	std::string m_tabEtlRule;				// 采集规则表
	std::string m_tabYLStatus;				// 详情表提交状态
	std::string m_tabCfgPfLfRela;			// 报表指标关联表
};

