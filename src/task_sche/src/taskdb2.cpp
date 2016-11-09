#include "taskdb2.h"
#include "log.h"


TaskDB2::TaskDB2(const DBInfo& db_info)
:base::BaseDB2(db_info.db_inst, db_info.db_user, db_info.db_pwd)
{
}

TaskDB2::~TaskDB2()
{
	//Disconnect();
}

void TaskDB2::SetTabTaskRequest(const std::string& tab_taskreq)
{
	m_tabTaskReq = tab_taskreq;
}

void TaskDB2::SetTabRaKpi(const std::string& tab_kpi)
{
	m_tabRaKpi = tab_kpi;
}

bool TaskDB2::IsTableExists(const std::string& tab_name) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		std::string sql = "select count(0) from syscat.tables where tabname='" + tab_name + "'";
		m_pLog->Output("[DB2] Check table: %s", tab_name.c_str());

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		int count = -1;
		while ( !rs.IsEOF() )
		{
			count = (int)rs[1];

			rs.MoveNext();
		}

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

