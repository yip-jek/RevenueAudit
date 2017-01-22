#include "ydtaskdb2.h"
#include "log.h"


YDTaskDB2::YDTaskDB2(const DBInfo& db_info)
:base::BaseDB2(db_info.db_inst, db_info.db_user, db_info.db_pwd)
{
}

YDTaskDB2::~YDTaskDB2()
{
	Disconnect();
}

void YDTaskDB2::SetTabTaskSche(const std::string& tab_tasksche)
{
	m_tabTaskSche = tab_tasksche;
}

void YDTaskDB2::SetTabKpiRule(const std::string& tab_kpirule)
{
	m_tabKpiRule = tab_kpirule;
}

void YDTaskDB2::SetTabEtlRule(const std::string& tab_etlrule)
{
	m_tabEtlRule = tab_etlrule;
}

bool YDTaskDB2::IsTableExists(const std::string& tab_name) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		std::string sql = "SELECT COUNT(0) FROM SYSCAT.TABLES WHERE TABNAME='" + tab_name + "'";
		m_pLog->Output("[DB2] Check table: %s", tab_name.c_str());

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		int count = -1;
		while ( !rs.IsEOF() )
		{
			count = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();

		if ( count < 0 )        // 没有返回结果
		{
			throw base::Exception(TDB_ERR_TAB_EXISTS, "[DB2] Check table '%s' whether exist or not failed: NO result! [FILE:%s, LINE:%d]", tab_name.c_str(), __FILE__, __LINE__);
		}
		else
		{
			return (count > 0);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_TAB_EXISTS, "[DB2] Check table '%s' whether exist or not failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

