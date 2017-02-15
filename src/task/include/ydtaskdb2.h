#pragma once

#include <map>
#include "basedb2.h"
#include "ydstruct.h"

class YDTaskDB2 : public base::BaseDB2
{
public:
	// 数据库错误代码
	enum YDTASK_DB_ERROR
	{
		TDB_ERR_TAB_EXISTS        = -20000001,				// 检查表存在性失败
		TDB_ERR_GET_TASKSCHE      = -20000002,				// 获取任务日程记录失败
		TDB_ERR_UPD_TASKSCHE_TIME = -20000003,				// 更新任务日程的任务时间失败
		TDB_ERR_GET_KPIRULE_SUBID = -20000004,				// 获取指标规则的子ID失败
		TDB_ERR_GET_TSLOG_MAXID   = -20000005,				// 获取任务日程日志表的MAX序号失败
		TDB_ERR_INS_TASKSCHELOG   = -20000006,				// 新增任务日程日志失败
		TDB_ERR_SEL_TSLOG_STATE   = -20000007,				// 查询任务日程日志状态失败
		TDB_ERR_UPD_ETL_TIME      = -20000008,				// 更新采集时间失败
		TDB_ERR_IS_TASKCHE_EXIST  = -20000009,				// 查看任务日程是否存在失败
		TDB_ERR_SET_TS_NOTACTIVE  = -20000010,				// 设置任务日程为未激活失败
		TDB_ERR_UPD_TASKSCHELOG   = -20000011,				// 更新任务日程日志失败
	};

public:
	YDTaskDB2(const DBInfo& db_info);
	virtual ~YDTaskDB2();

public:
	// 设置任务日程表
	void SetTabTaskSche(const std::string& tab_tasksche);

	// 设置任务日程日志表
	void SetTabTaskScheLog(const std::string& tab_taskschelog);

	// 设置指标规则表
	void SetTabKpiRule(const std::string& tab_kpirule);

	// 设置采集规则表
	void SetTabEtlRule(const std::string& tab_etlrule);

	// 表是否存在
	bool IsTableExists(const std::string& tab_name) throw(base::Exception);

	// 获取任务日程记录
	void GetTaskSchedule(std::map<int, TaskSchedule>& m_tasksche) throw(base::Exception);

	// 任务日程是否存在？
	bool IsTaskScheExist(int id) throw(base::Exception);

	// 更新任务日程的任务时间
	void UpdateTaskScheTaskTime(int id, const std::string& start_time, const std::string& finish_time) throw(base::Exception);

	// 设置任务日程为未激活
	void SetTaskScheNotActive(int id) throw(base::Exception);

	// 获取指标规则的子ID
	void GetKpiRuleSubID(const std::string& kpi_id, std::string& etl_id, std::string& ana_id) throw(base::Exception);

	// 获取任务日程日志表的MAX序号
	int GetTaskScheLogMaxID() throw(base::Exception);

	// 新增任务日程日志
	void InsertTaskScheLog(const TaskScheLog& ts_log) throw(base::Exception);

	// 查询任务日程日志状态
	void SelectTaskScheLogState(TaskScheLog& ts_log) throw(base::Exception);

	// 更新采集时间
	void UpdateEtlTime(const std::string& etl_id, const std::string& etl_time) throw(base::Exception);

	// 更新任务日程日志
	void UpdateTaskScheLog(const TaskScheLog& ts_log) throw(base::Exception);

private:
	std::string m_tabTaskSche;				// 任务日程表
	std::string m_tabTaskScheLog;			// 任务日程日志表
	std::string m_tabKpiRule;				// 指标规则表
	std::string m_tabEtlRule;				// 采集规则表
};

