#include "chivethrift.h"
#include <vector>
#include "log.h"

CAcqHive::CAcqHive(const std::string& ip, int port)
:BaseJHive(ip, port)
{
}

CAcqHive::~CAcqHive()
{
}

void CAcqHive::RebuildTable(const std::string& tab_name, std::vector<std::string>& vec_field) throw(base::Exception)
{
	if ( tab_name.empty() )
	{
		throw base::Exception(HTERR_REBUILD_TABLE_FAILED, "[HIVE] Can not rebuild! 库表名为空! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( vec_field.empty() )
	{
		throw base::Exception(HTERR_REBUILD_TABLE_FAILED, "[HIVE] 没有库表 [%s] 的字段信息! [FILE:%s, LINE:%d]", tab_name.c_str(), __FILE__, __LINE__);
	}

	std::string sql_drop = "DROP TABLE IF EXISTS " + tab_name;

	std::string sql_create = "CREATE TABLE " + tab_name + " ( ";
	const size_t VEC_SIZE = vec_field.size();
	for ( size_t i = 0; i < VEC_SIZE; ++i )
	{
		std::string& str_field = vec_field[i];

		if ( str_field.empty() )
		{
			throw base::Exception(HTERR_REBUILD_TABLE_FAILED, "[HIVE] 缺少库表 [%s] 的第 [%lu] 个字段的信息! [FILE:%s, LINE:%d]", tab_name.c_str(), (i+1), __FILE__, __LINE__);
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

	try
	{
		m_pLog->Output("[HIVE] Try to drop table: %s", tab_name.c_str());
		ExecuteSQL(sql_drop);
		m_pLog->Output("[HIVE] Drop table OK.");

		m_pLog->Output("[HIVE] Create table: %s", tab_name.c_str());
		ExecuteSQL(sql_create);
		m_pLog->Output("[HIVE] Create table ---- done!");
	}
	catch ( const apache::thrift::TApplicationException& ex )
	{
		throw base::Exception(HTERR_REBUILD_TABLE_FAILED, "[HIVE] Rebuild table \"%s\" failed! [TApplicationException] %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.what(), __FILE__, __LINE__);
	}
	catch ( const apache::thrift::TException& ex )
	{
		throw base::Exception(HTERR_REBUILD_TABLE_FAILED, "[HIVE] Rebuild table \"%s\" failed! [TException] %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAcqHive::ExecuteAcqSQL(std::vector<std::string>& vec_sql) throw(base::Exception)
{
	if ( vec_sql.empty() )
	{
		throw base::Exception(HTERR_EXECUTE_ACQSQL_FAILED, "[HIVE] No sql to be executed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	try
	{
		const size_t VEC_SIZE = vec_sql.size();
		for ( size_t i = 0; i < VEC_SIZE; ++i )
		{
			std::string& hive_sql = vec_sql[i];

			m_pLog->Output("[HIVE] Execute sql [%lu]: %s", (i+1), hive_sql.c_str());
			m_spHiveClient->execute(hive_sql);
			m_pLog->Output("[HIVE] Execute sql OK.");
		}
	}
	catch ( const apache::thrift::TApplicationException& ex )
	{
		throw base::Exception(HTERR_EXECUTE_ACQSQL_FAILED, "[HIVE] Execute sql failed! [TApplicationException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
	catch ( const apache::thrift::TException& ex )
	{
		throw base::Exception(HTERR_EXECUTE_ACQSQL_FAILED, "[HIVE] Execute sql failed! [TException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

