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

void TaskDB2::SetTabEtlRule(const std::string& tab_etlrule)
{
	m_tabEtlRule = tab_etlrule;
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

void TaskDB2::SelectNewTaskRequest(std::vector<TaskReqInfo>& vec_trinfo) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	TaskReqInfo tr_info;
	std::vector<TaskReqInfo> v_tri;

	std::string sql = "select SEQ_ID, KPI_ID, STAT_CYCLE from " + m_tabTaskReq + " where TASK_STATUS = '00'";
	m_pLog->Output("[DB2] Select new task request: %s", sql.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			tr_info.seq_id      = (int)rs[index++];
			tr_info.kpi_id      = (const char*)rs[index++];
			tr_info.stat_cycle  = (const char*)rs[index++];

			rs.MoveNext();

			tr_info.status.clear();
			tr_info.status_desc.clear();
			tr_info.gentime.clear();
			tr_info.finishtime.clear();
			tr_info.desc.clear();
			v_tri.push_back(tr_info);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_TAB_EXISTS, "[DB2] Check table '%s' whether exist or not failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_tri.swap(vec_trinfo);
}

