#pragma once

#include <string>
#include <vector>

// 数据库信息 [DB2]
struct AnaDBInfo
{
public:
	AnaDBInfo(): time_stamp(false)
	{}

	AnaDBInfo(const AnaDBInfo& info)
	:target_table(info.target_table)
	,db2_sql(info.db2_sql)
	,time_stamp(info.time_stamp)
	,vec_fields(info.vec_fields)
	{}

	const AnaDBInfo& operator = (const AnaDBInfo& info)
	{
		if ( this != &info )
		{
			this->target_table = info.target_table;
			this->db2_sql      = info.db2_sql;
			this->time_stamp   = info.time_stamp;
			this->vec_fields   = info.vec_fields;
		}

		return *this;
	}

public:
	std::string					target_table;		// 最终目标表名
	std::string					db2_sql;			// 数据库SQL语句
	bool						time_stamp;			// 入库是否带时间戳
	std::vector<std::string>	vec_fields;			// 目标表字段信息
};

