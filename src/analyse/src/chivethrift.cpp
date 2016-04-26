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

void CHiveThrift::Test(const std::string& table) throw(base::Exception)
{
	try
	{
		std::string test_sql = "select * from " + table;
		m_pLog->Output("[HIVE] Query sql: %s", test_sql.c_str());

		m_pLog->Output("[HIVE] Execute query sql ...");
		m_spHiveClient->execute(test_sql);
		m_pLog->Output("[HIVE] Execute query sql OK.");

		std::vector<std::string> vec_str;
		long total = 0;
		do
		{
			vec_str.clear();

			m_spHiveClient->fetchN(vec_str, HIVE_MAX_FETCHN);

			const int V_SIZE = vec_str.size();
			for ( int i = 0; i < V_SIZE; ++i )
			{
				m_pLog->Output("[GET] %d> %s", ++total, vec_str[i].c_str());
			}
		} while ( vec_str.size() > 0 );
		m_pLog->Output("[HIVE] Get %ld row(s)", total);
	}
	catch ( const apache::thrift::TApplicationException& ex )
	{
		throw base::Exception(HTERR_APP_EXCEPTION, "[TApplicationException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
	catch ( const apache::thrift::TException& ex )
	{
		throw base::Exception(HTERR_T_EXCEPTION, "[TException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

void CHiveThrift::DropHiveTable(const std::string& tab_name)
{
	try
	{
		std::string sql = "DROP TABLE IF EXISTS " + tab_name;
		m_pLog->Output("[HIVE] Try to drop table: %s", tab_name.c_str());

		m_spHiveClient->execute(sql);
		m_pLog->Output("[HIVE] Drop table OK.");
	}
	catch ( const apache::thrift::TApplicationException& ex )
	{
		throw base::Exception(HTERR_APP_EXCEPTION, "[TApplicationException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
	catch ( const apache::thrift::TException& ex )
	{
		throw base::Exception(HTERR_T_EXCEPTION, "[TException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

void CHiveThrift::ExecuteSQL(const std::string& hive_sql) throw(base::Exception)
{
	try
	{
		m_pLog->Output("[HIVE] Query sql: %s", hive_sql.c_str());

		m_spHiveClient->execute(hive_sql);
		m_pLog->Output("[HIVE] Execute query sql OK.");
	}
	catch ( const apache::thrift::TApplicationException& ex )
	{
		throw base::Exception(HTERR_APP_EXCEPTION, "[TApplicationException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
	catch ( const apache::thrift::TException& ex )
	{
		throw base::Exception(HTERR_T_EXCEPTION, "[TException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

