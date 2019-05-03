#pragma once

#include "basejhive.h"

class CAcqHive : public base::BaseJHive
{

public:
	CAcqHive(const std::string& hive_jclassname);
	virtual ~CAcqHive();

public:
	// 重建表数据
	void RebuildTable(const std::string& tab_name, std::vector<std::string>& vec_field, const std::string& tab_location);

	// 执行采集Hive SQL 
	void ExecuteAcqSQL(std::vector<std::string>& vec_sql);

	// 表是否存在
	// 返回：true-表存在，false-表不存在
	bool CheckTableExisted(const std::string& tab_name);
};

