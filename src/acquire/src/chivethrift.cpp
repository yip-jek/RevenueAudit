#include "chivethrift.h"
#include <vector>
#include "log.h"

CHiveThrift::CHiveThrift(const std::string& ip, int port)
:BaseHiveThrift(ip, port)
{
}

CHiveThrift::~CHiveThrift()
{
}

//void CHiveThrift::Test(const std::string& table) throw(base::Exception)
//{
//	try
//	{
//		std::string test_sql = "select * from " + table;
//		m_pLog->Output("[HIVE] Query sql: %s", test_sql.c_str());
//
//		m_pLog->Output("[HIVE] Execute query sql ...");
//		m_spHiveClient->execute(test_sql);
//		m_pLog->Output("[HIVE] Execute query sql OK.");
//
//		std::vector<std::string> vec_str;
//		long total = 0;
//		do
//		{
//			vec_str.clear();
//
//			m_spHiveClient->fetchN(vec_str, HIVE_MAX_FETCHN);
//
//			const int V_SIZE = vec_str.size();
//			for ( int i = 0; i < V_SIZE; ++i )
//			{
//				m_pLog->Output("[GET] %d> %s", ++total, vec_str[i].c_str());
//			}
//		} while ( vec_str.size() > 0 );
//		m_pLog->Output("[HIVE] Get %ld row(s)", total);
//	}
//	catch ( const apache::thrift::TApplicationException& ex )
//	{
//		throw base::Exception(HTERR_APP_EXCEPTION, "[HIVE] [TApplicationException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
//	}
//	catch ( const apache::thrift::TException& ex )
//	{
//		throw base::Exception(HTERR_T_EXCEPTION, "[HIVE] [TException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
//	}
//}

void CHiveThrift::RebuildTable(const std::string& tab_name, std::vector<std::string>& vec_field) throw(base::Exception)
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
		m_spHiveClient->execute(sql_drop);
		m_pLog->Output("[HIVE] Drop table OK.");

		m_pLog->Output("[HIVE] Create table [%s]: %s", tab_name.c_str(), sql_create.c_str());
		m_spHiveClient->execute(sql_create);
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

void CHiveThrift::ExecuteAcqSQL(std::vector<std::string>& vec_sql) throw(base::Exception)
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

