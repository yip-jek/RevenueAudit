#pragma once

#include "basejhive.h"

class CAcqHive : public base::BaseJHive
{

public:
	CAcqHive(const std::string& ip, int port);
	virtual ~CAcqHive();

public:
	// 错误码（枚举）
	enum HT_ERROR
	{
		HTERR_REBUILD_TABLE_FAILED  = -2001001,			// 重建Hive表失败
		HTERR_EXECUTE_ACQSQL_FAILED = -2001002,			// 执行采集SQL失败
	};

public:
	// 重建表数据
	void RebuildTable(const std::string& tab_name, std::vector<std::string>& vec_field) throw(base::Exception);

	// 执行采集Hive SQL 
	void ExecuteAcqSQL(std::vector<std::string>& vec_sql) throw(base::Exception);
};

