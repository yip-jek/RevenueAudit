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

void YDTaskDB2::GetTaskSchedule(std::map<int, TaskSchedule>& m_tasksche) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT SEQ_ID, ACTIVATE, TASK_TYPE, KPI_ID, TASK_CYCLE, TASK_STATE, ";
	sql += "TASK_STATE_DESC, EXPIRY_DATE_START, EXPIRY_DATE_END FROM " + m_tabTaskSche;
	m_pLog->Output("[DB2] Get task schedule: %s", sql.c_str());

	TaskSchedule task_sche;
	std::map<int, TaskSchedule> m_ts;

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			task_sche.seq_id            = (int)rs[index++];
			task_sche.activate          = (char)rs[index++];
			task_sche.task_type         = (const char*)rs[index++];
			task_sche.kpi_id            = (const char*)rs[index++];
			task_sche.task_cycle        = (const char*)rs[index++];
			task_sche.task_state        = (const char*)rs[index++];
			task_sche.task_state_desc   = (const char*)rs[index++];
			task_sche.expiry_date_start = (const char*)rs[index++];
			task_sche.expiry_date_end   = (const char*)rs[index++];

			m_ts[task_sche.seq_id] = task_sche;

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_GET_TASKSCHE, "[DB2] Get task schedule from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskSche.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_ts.swap(m_tasksche);
}

