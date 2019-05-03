#pragma once

#include "basejhive.h"

class CAnaHive : public base::BaseJHive
{

public:
	CAnaHive(const std::string& hive_jclassname);
	virtual ~CAnaHive();

public:
	// 获取源数据
	void FetchSourceData(const std::string& hive_sql, std::vector<std::vector<std::string> >& vec2_fields);

	// 执行分析 HIVE SQL
	void ExecuteAnaSQL(const std::string& hive_sql);
};

