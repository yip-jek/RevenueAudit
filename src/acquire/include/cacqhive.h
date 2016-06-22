#pragma once

#include "basejhive.h"

class CAcqHive : public base::BaseJHive
{

public:
	//CAcqHive(const std::string& ip, int port, const std::string& usr, const std::string& pwd);
	CAcqHive();
	virtual ~CAcqHive();

public:
	// 错误码（枚举）
	enum HT_ERROR
	{
		HTERR_REBUILD_TABLE_FAILED     = -2001001,			// 重建Hive表失败
		HTERR_EXECUTE_ACQSQL_FAILED    = -2001002,			// 执行采集SQL失败
		HTERR_CHECK_TAB_EXISTED_FAILED = -2001003,			// 检查表是否存在失败
	};

public:
	// 重建表数据
	void RebuildTable(const std::string& tab_name, std::vector<std::string>& vec_field) throw(base::Exception);

	// 执行采集Hive SQL 
	void ExecuteAcqSQL(std::vector<std::string>& vec_sql) throw(base::Exception);

	// 表是否存在
	// 返回：true-表存在，false-表不存在
	bool CheckTableExisted(const std::string& tab_name) throw(base::Exception);
};

