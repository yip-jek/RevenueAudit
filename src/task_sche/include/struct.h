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
	std::string kpi_id;					// 指标ID
	std::string stat_cycle;				// 稽核账期
	std::string status;					// 任务状态
	std::string status_desc;			// 任务状态说明
	std::string gentime;				// 任务生成时间
	std::string finishtime;				// 任务完成时间
	std::string desc;					// 备注
};

// 指标规则信息
struct RaKpiInfo
{
	std::string kpi_id;							// 指标ID
	std::string ana_id;							// 分析ID
	std::vector<std::string> vec_etl_id;		// 采集ID集（可能多个采集）
};

