#pragma once

#include <string>
#include <vector>
#include "pubstr.h"

// 任务请求信息
struct YCTaskReq
{
public:
	YCTaskReq(): seq(0), task_batch(0)
	{}

public:
	int         seq;				// 任务流水号
	std::string state;				// 任务状态
	std::string state_desc;			// 任务状态说明
	int         task_batch;			// 任务批次
	std::string task_desc;			// 备注
};

// 统计结果批次信息
struct YCStatBatch
{
public:
	YCStatBatch(): stat_batch(0) {}

public:
	std::string stat_report;			// 关联报表
	std::string stat_id;				// 统计指标ID
	std::string stat_date;				// 统计账期
	std::string stat_city;				// 统计地市
	int         stat_batch;				// 统计批次
};

// 业财稽核因子规则信息
struct YCStatInfo
{
public:
	// 优先级类型
	enum STAT_PRIORITY
	{
		SP_Unknown = 0,				// 未知
		SP_Level_0 = 1,				// 优先级_0（00-一般因子)
		SP_Level_1 = 2,				// 优先级_1（01-组合因子)
		SP_Level_2 = 3,				// 优先级_2（02-汇总因子)
	};

public:
	YCStatInfo(): stat_pri(SP_Unknown) {}

public:
	// 设置优先级别
	bool SetStatPriority(const std::string& st_p)
	{
		const std::string SP = base::PubStr::TrimUpperB(st_p);

		if ( "00" == SP )
		{
			stat_pri = SP_Level_0;
		}
		else if ( "01" == SP )
		{
			stat_pri = SP_Level_1;
		}
		else if ( "02" == SP )
		{
			stat_pri = SP_Level_2;
		}
		else
		{
			stat_pri = SP_Unknown;
			return false;
		}

		return true;
	}

public:
	std::string   stat_id;					// 统计指标ID
	std::string   stat_name;				// 统计指标名称
	std::string   statdim_id;				// 统计维度ID
	STAT_PRIORITY stat_pri;					// 优先级别
	std::string   stat_sql;					// 统计SQL
	std::string   stat_report;				// 关联报表
};

// 业财稽核因子结果信息
struct YCStatResult
{
public:
	YCStatResult(): stat_batch(0), stat_value(0.0)
	{}

public:
	// 数据转换 -> std::vector<std::string>
	void Trans2Vector(std::vector<std::string>& vec_str)
	{
		std::vector<std::string> v_str;
		v_str.push_back(stat_report);
		v_str.push_back(stat_id);
		v_str.push_back(stat_name);
		v_str.push_back(statdim_id);
		v_str.push_back(stat_city);
		v_str.push_back(base::PubStr::Int2Str(stat_batch));
		v_str.push_back(base::PubStr::Double2Str(stat_value));

		v_str.swap(vec_str);
	}

public:
	std::string stat_report;				// 关联报表
	std::string stat_id;					// 统计指标ID
	std::string stat_name;					// 统计指标名称
	std::string statdim_id;					// 统计维度ID
	std::string stat_city;					// 地市
	int         stat_batch;					// 批次
	double      stat_value;					// 统计维度值
};

// 业财稽核日志信息
struct YCStatLog
{
public:
	YCStatLog(): stat_batch(0) {}

public:
	std::string stat_report;				// 业财报表
	int         stat_batch;					// 稽核批次
	std::string stat_datasource;			// 稽核数据源及批次
	std::string stat_city;					// 稽核地市
	std::string stat_cycle;					// 稽核账期
	std::string stat_time;					// 稽核时间
};

// 业财稽核数据源信息
struct YCSrcInfo
{
public:
	YCSrcInfo(): batch(0) {}

public:
	std::string src_tab;				// 数据源表名
	std::string field_period;			// 账期字段名
	std::string period;					// 账期
	std::string field_city;				// 地市字段名
	std::string city;					// 地市
	std::string field_batch;			// 批次字段名
	int         batch;					// 批次
};

