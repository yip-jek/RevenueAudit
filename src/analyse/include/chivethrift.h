#pragma once

#include "basehivethrift.h"

class CHiveThrift : public base::BaseHiveThrift
{

public:
	CHiveThrift(const std::string& ip, int port);
	virtual ~CHiveThrift();

public:
	// 一次获取的最大HIVE记录数
	static const int HIVE_MAX_FETCHN = 40960;

	// 错误码（枚举）
	enum HT_ERROR
	{
		HTERR_FETCH_SRCDATA_FAILED = -3001001,			// 获取源数据失败
	};

public:
	//// 测试
	//void Test(const std::string& table) throw(base::Exception);

	// 获取源数据
	void FetchSourceData(const std::string& hive_sql, std::vector<std::vector<std::string> >& vec2_fields) throw(base::Exception);
};

