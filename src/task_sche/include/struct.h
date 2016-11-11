#pragma once

#include <string>
#include <vector>

// 数据库信息
struct DBInfo
{
	std::string db_inst;				// 数据库实例名
	std::string db_user;				// 数据库用户名
	std::string db_pwd;					// 数据库密码
};

// 任务请求信息
struct TaskReqInfo
{
public:
	TaskReqInfo(): seq_id(0)
	{}

public:
	int         seq_id;					// 流水号
	std::string kpi_id;					// 指标规则ID
	std::string stat_cycle;				// 稽核账期
	std::string status;					// 任务状态
	std::string status_desc;			// 任务状态说明
	std::string gentime;				// 任务生成时间
	std::string finishtime;				// 任务完成时间
	std::string desc;					// 备注
};

// 指标规则信息
struct KpiRuleInfo
{
	std::string kpi_id;					// 指标规则ID
	std::string etl_id;					// 采集规则ID
	std::string ana_id;					// 分析规则ID
};

// 任务信息
struct TaskInfo
{
public:
	// 任务类型
	enum TASK_TYPE
	{
		TT_Unknown = 0,					// 未知类型（预设）
		TT_Acquire = 1,					// 采集类型
		TT_Analyse = 2,					// 分析类型
	};

public:
	TaskInfo(): t_type(TT_Unknown), task_id(0)
	{}

public:
	TASK_TYPE   t_type;					// 任务类型
	long long   task_id;				// 任务ID
	std::string kpi_id;					// 指标规则ID
	std::string sub_id;					// 子规则ID（采集规则ID 或 分析规则ID，视乎任务类型而定）
	std::string etl_time;				// 采集时间
};

