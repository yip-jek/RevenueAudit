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

// （业财稽核）分类维度因子对
struct YCCategoryFactor
{
public:
	std::string dim_id;					// 维度ID
	std::string item;					// 数据项名称
	std::string value;					// 维度值
};

// （业财稽核）两个分类维度因子对
struct YCPairCategoryFactor
{
public:
	YCPairCategoryFactor(): index(0)
	{}

public:
	int              index;				// 序号
	YCCategoryFactor cf_A;				// （左A）分类维度因子对
	YCCategoryFactor cf_B;				// （右B）分类维度因子对
};

// （业财稽核）因子规则信息
struct YCStatInfo
{
public:
	YCStatInfo(): category(false)
	{}

public:
	bool        category;				// 分类因子标志
	std::string stat_id;				// 统计指标ID
	std::string stat_name;				// 统计指标名称
	std::string statdim_id;				// 统计维度ID
	std::string stat_priority;			// 优先级别
	std::string stat_sql;				// 统计SQL
	std::string stat_report;			// 关联报表
};

// （业财稽核）因子结果信息
struct YCStatResult
{
public:
	YCStatResult(): stat_batch(0)
	{}

	static const int S_NUMBER_OF_MEMBERS = 7;			// 总成员数

public:
	// 载入数据
	bool LoadFromVector(std::vector<std::string>& vec_str)
	{
		const int VEC_SIZE = vec_str.size();
		if ( VEC_SIZE != S_NUMBER_OF_MEMBERS )
		{
			return false;
		}
	
		int index = 0;
		stat_report = vec_str[index++];
		stat_id     = vec_str[index++];
		stat_name   = vec_str[index++];
		statdim_id  = vec_str[index++];
		stat_city   = vec_str[index++];
	
		if ( !base::PubStr::Str2Int(vec_str[index++], stat_batch) )
		{
			return false;
		}
	
		stat_value  = vec_str[index++];
		return true;
	}

	// 数据转换 -> std::vector<std::string>
	void Convert2Vector(std::vector<std::string>& vec_str)
	{
		std::vector<std::string> v_str;
		v_str.push_back(stat_report);
		v_str.push_back(stat_id);
		v_str.push_back(stat_name);
		v_str.push_back(statdim_id);
		v_str.push_back(stat_city);
		v_str.push_back(base::PubStr::Int2Str(stat_batch));
		v_str.push_back(stat_value);

		v_str.swap(vec_str);
	}

public:
	std::string stat_report;				// 关联报表
	std::string stat_id;					// 统计指标ID
	std::string stat_name;					// 统计指标名称
	std::string statdim_id;					// 统计维度ID
	std::string stat_city;					// 地市
	int         stat_batch;					// 批次
	std::string stat_value;					// 统计维度值
};

// （业财稽核）日志信息
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

// （业财稽核）数据源信息
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

