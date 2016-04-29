#pragma once

#include "basehivethrift.h"

class CHiveThrift : public base::BaseHiveThrift
{

public:
	CHiveThrift(const std::string& ip, int port);
	virtual ~CHiveThrift();

public:
	static const int HIVE_MAX_FETCHN = 20000;

	enum HT_ERROR
	{
		HTERR_REBUILD_TABLE_FAILED  = -2001001,			// 重建Hive表失败
		HTERR_EXECUTE_ACQSQL_FAILED = -2001002,			// 执行采集SQL失败
	};

public:
	//void Test(const std::string& table) throw(base::Exception);

	// 重建表数据
	void RebuildHiveTable(const std::string& tab_name, std::vector<std::string>& vec_field) throw(base::Exception);

	// 执行采集Hive SQL 
	void ExecuteAcqSQL(const std::string& hive_sql) throw(base::Exception);
};

