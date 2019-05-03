#include "cacqhive.h"
#include <vector>
#include "log.h"
#include "acqerror.h"

CAcqHive::CAcqHive(const std::string& hive_jclassname)
:base::BaseJHive(hive_jclassname)
{
}

CAcqHive::~CAcqHive()
{
}

void CAcqHive::RebuildTable(const std::string& tab_name, std::vector<std::string>& vec_field, const std::string& tab_location)
{
	if ( tab_name.empty() )
	{
		throw base::Exception(ACQERR_REBUILD_TABLE, "[HIVE] Can not rebuild! 库表名为空! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( vec_field.empty() )
	{
		throw base::Exception(ACQERR_REBUILD_TABLE, "[HIVE] 没有库表 [%s] 的字段信息! [FILE:%s, LINE:%d]", tab_name.c_str(), __FILE__, __LINE__);
	}

	std::string sql_drop = "DROP TABLE IF EXISTS " + tab_name;

	std::string sql_create = "CREATE TABLE " + tab_name + " ( ";
	const size_t VEC_SIZE = vec_field.size();
	for ( size_t i = 0; i < VEC_SIZE; ++i )
	{
		std::string& str_field = vec_field[i];

		if ( str_field.empty() )
		{
			throw base::Exception(ACQERR_REBUILD_TABLE, "[HIVE] 缺少库表 [%s] 的第 [%lu] 个字段的信息! [FILE:%s, LINE:%d]", tab_name.c_str(), (i+1), __FILE__, __LINE__);
		}

		if ( i != 0 )
		{
			sql_create += ", " + str_field + " string";
		}
		else
		{
			sql_create += str_field + " string";
		}
	}
	sql_create += " ) row format delimited fields terminated by '|' stored as textfile";

	// 测试环境：不指定建表的 location
	if ( !tab_location.empty() )
	{
		// 指定建表的 location
		sql_create += " location '" + tab_location + tab_name + "'";
	}

	try
	{
		m_pLog->Output("[HIVE] Try to drop table: %s", tab_name.c_str());
		ExecuteSQL(sql_drop);
		m_pLog->Output("[HIVE] Drop table OK.");

		m_pLog->Output("[HIVE] Create table: %s", tab_name.c_str());
		ExecuteSQL(sql_create);
		m_pLog->Output("[HIVE] Create table ---- done!");
	}
	catch ( const base::Exception& ex )
	{
		throw base::Exception(ACQERR_REBUILD_TABLE, "[HIVE] Rebuild table \"%s\" failed! %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.What().c_str(), __FILE__, __LINE__);
	}
}

void CAcqHive::ExecuteAcqSQL(std::vector<std::string>& vec_sql)
{
	if ( vec_sql.empty() )
	{
		throw base::Exception(ACQERR_EXECUTE_ACQSQL, "[HIVE] No sql to be executed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	try
	{
		const size_t VEC_SIZE = vec_sql.size();
		for ( size_t i = 0; i < VEC_SIZE; ++i )
		{
			std::string& hive_sql = vec_sql[i];

			ExecuteSQL(hive_sql);
		}
	}
	catch ( const base::Exception& ex )
	{
		throw base::Exception(ACQERR_EXECUTE_ACQSQL, "[HIVE] Execute sql failed! %s [FILE:%s, LINE:%d]", ex.What().c_str(), __FILE__, __LINE__);
	}
}

bool CAcqHive::CheckTableExisted(const std::string& tab_name)
{
	try
	{
		std::string sql = "show tables like '" + tab_name + "'";
		m_pLog->Output("[HIVE] Check table: %s", tab_name.c_str());

		std::vector<std::vector<std::string> > vec2_str;
		FetchSQL(sql, vec2_str);

		return (!vec2_str.empty());
	}
	catch ( const base::Exception& ex )
	{
		throw base::Exception(ACQERR_CHECK_TAB_EXISTED, "[HIVE] Execute sql failed! %s [FILE:%s, LINE:%d]", ex.What().c_str(), __FILE__, __LINE__);
	}
}

