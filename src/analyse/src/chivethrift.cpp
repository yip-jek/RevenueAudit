#include "chivethrift.h"
#include <vector>
#include "log.h"
#include "pubstr.h"

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
//		throw base::Exception(HTERR_APP_EXCEPTION, "[TApplicationException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
//	}
//	catch ( const apache::thrift::TException& ex )
//	{
//		throw base::Exception(HTERR_T_EXCEPTION, "[TException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
//	}
//}

void CHiveThrift::FetchSourceData(const std::string& hive_sql, const size_t& total_num_of_fields, std::vector<std::vector<std::string> >& vec2_fields) throw(base::Exception)
{
	if ( hive_sql.empty() )
	{
		throw base::Exception(HTERR_FETCH_SRCDATA_FAILED, "[HIVE] Fetch source data failed: no sql to be executed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	std::vector<std::vector<std::string> > vv_fields;

	size_t total_count = 0;
	try
	{
		m_pLog->Output("[HIVE] Execute fetch sql: %s", hive_sql.c_str());
		m_spHiveClient->execute(hive_sql);
		m_pLog->Output("[HIVE] Execute fetch sql OK!");

		std::vector<std::string> v_srcdata;
		v_srcdata.reserve(HIVE_MAX_FETCHN);

		m_pLog->Output("[HIVE] Fetch source data ...");
		std::vector<std::string> v_field;

		while ( true )
		{
			v_srcdata.clear();

			m_spHiveClient->fetchN(v_srcdata, HIVE_MAX_FETCHN);

			const size_t V_SRC_SIZE = v_srcdata.size();
			for ( size_t i = 0; i < V_SRC_SIZE; ++i )
			{
				std::string& ref_srcdata = v_srcdata[i];

				if ( ref_srcdata.empty() )
				{
					throw base::Exception(HTERR_FETCH_SRCDATA_FAILED, "[HIVE] Fetch source data failed: source data is a blank! (Record_seq: %llu) [FILE:%s, LINE:%d]", (total_count+i+1), __FILE__, __LINE__);
				}

				base::PubStr::Str2StrVector(ref_srcdata, "\t", v_field);
				if ( v_field.size() != total_num_of_fields )
				{
					throw base::Exception(HTERR_FETCH_SRCDATA_FAILED, "[HIVE] Fetch source data failed: 源数据的字段个数 (%lu) 与目标表的字段个数 (%lu) 不匹配! (Record_seq: %llu) [FILE:%s, LINE:%d]", v_field.size(), total_num_of_fields, (total_count+i+1), __FILE__, __LINE__);
				}

				base::PubStr::VVectorSwapPushBack(vv_fields, v_field);
			}

			if ( V_SRC_SIZE > 0 )
			{
				total_count += V_SRC_SIZE;
				m_pLog->Output("[HIVE] Fetch count: %llu", total_count);
			}
			else
			{
				break;
			}
		}

		m_pLog->Output("[HIVE] Fetch source data ---- done!");
	}
	catch ( const apache::thrift::TApplicationException& ex )
	{
		throw base::Exception(HTERR_FETCH_SRCDATA_FAILED, "[HIVE] Fetch source data failed! [TApplicationException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
	catch ( const apache::thrift::TException& ex )
	{
		throw base::Exception(HTERR_FETCH_SRCDATA_FAILED, "[HIVE] Fetch source data failed! [TException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == total_count )
	{
		throw base::Exception(HTERR_FETCH_SRCDATA_FAILED, "[HIVE] Fetch source data failed: no record! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	vv_fields.swap(vec2_fields);
}

