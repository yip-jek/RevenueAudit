#pragma once

#include <string>
#include <vector>
#include "pubtime.h"

// 时间字段
struct TimeField
{
public:
	TimeField(): valid(false), index(-1)
	{}

public:
	bool        valid;					// 是否有效
	int         index;					// 所在位置索引
	std::string str_time;
};

struct AnaField
{
	std::string field_name;				// 字段名
	std::string CN_name;				// 中文名
};

// 数据库信息 [DB2]
struct AnaDBInfo
{
public:
	AnaDBInfo(): date_type(base::PubTime::DT_UNKNOWN), val_beg_pos(0)
	{}

	AnaDBInfo(const AnaDBInfo& info)
	:target_table(info.target_table)
	,backup_table(info.backup_table)
	,db2_sql(info.db2_sql)
	,tf_etlday(info.tf_etlday)
	,tf_nowday(info.tf_nowday)
	,date_type(info.date_type)
	,date_time(info.date_time)
	,vec_fields(info.vec_fields)
	,val_beg_pos(info.val_beg_pos)
	{}

	const AnaDBInfo& operator = (const AnaDBInfo& info)
	{
		if ( this != &info )
		{
			this->target_table = info.target_table;
			this->backup_table = info.backup_table;
			this->db2_sql      = info.db2_sql;
			this->tf_etlday    = info.tf_etlday;
			this->tf_nowday    = info.tf_nowday;
			this->date_type    = info.date_type;
			this->date_time    = info.date_time;
			this->vec_fields   = info.vec_fields;
			this->val_beg_pos  = info.val_beg_pos;
		}

		return *this;
	}

public:
	std::string              target_table;				// 最终目标表名
	std::string              backup_table;				// 目标表的备份表（仅用于报表统计）
	std::string              db2_sql;					// 数据库SQL语句
	TimeField                tf_etlday;					// 采集时间字段
	TimeField                tf_nowday;					// 当前时间字段（格式：YYYYMMDD）
	base::PubTime::DATE_TYPE date_type;					// 时间类型
	std::string              date_time;					// 时间戳
	std::vector<AnaField>    vec_fields;				// 目标表字段信息
	int                      val_beg_pos;				// 值的开始序号
};

