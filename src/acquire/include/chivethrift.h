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
		HTERR_APP_EXCEPTION     = -2001001,			// TApplicationException异常
		HTERR_T_EXCEPTION       = -2001002,			// TException异常
	};

public:
	void Test(const std::string& table) throw(base::Exception);

	void DropHiveTable(const std::string& tab_name);

	void ExecuteSQL(const std::string& hive_sql) throw(base::Exception);
};

