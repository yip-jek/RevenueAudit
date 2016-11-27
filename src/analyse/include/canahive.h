#pragma once

#include "basejhive.h"

class CAnaHive : public base::BaseJHive
{

public:
	CAnaHive(const std::string& hive_jclassname);
	virtual ~CAnaHive();

public:
	// 错误码（枚举）
	enum HT_ERROR
	{
		HTERR_FETCH_SRCDATA_FAILED = -3001001,			// 获取源数据失败
	};

public:
	// 获取源数据
	void FetchSourceData(const std::string& hive_sql, std::vector<std::vector<std::string> >& vec2_fields) throw(base::Exception);
};

