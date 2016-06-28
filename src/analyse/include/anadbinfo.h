#pragma once

#include <string>
#include <vector>

// 数据库信息 [DB2]
struct AnaDBInfo
{
public:
	AnaDBInfo(): time_stamp(false), val_beg_pos(0)
	{}

	AnaDBInfo(const AnaDBInfo& info)
	:target_table(info.target_table)
	,db2_sql(info.db2_sql)
	,time_stamp(info.time_stamp)
	,date_time(info.date_time)
	,vec_fields(info.vec_fields)
	,val_beg_pos(info.val_beg_pos)
	{}

	const AnaDBInfo& operator = (const AnaDBInfo& info)
	{
		if ( this != &info )
		{
			this->target_table = info.target_table;
			this->db2_sql      = info.db2_sql;
			this->time_stamp   = info.time_stamp;
			this->date_time    = info.date_time;
			this->vec_fields   = info.vec_fields;
			this->val_beg_pos  = info.val_beg_pos;
		}

		return *this;
	}

public:
	std::string					target_table;		// 最终目标表名
	std::string					db2_sql;			// 数据库SQL语句
	bool						time_stamp;			// 入库是否带时间戳
	std::string					date_time;			// 时间戳
	std::vector<std::string>	vec_fields;			// 目标表字段信息
	int							val_beg_pos;		// 值的开始序号
};

