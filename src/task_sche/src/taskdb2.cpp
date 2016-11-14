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

void TaskDB2::SetTabKpiRule(const std::string& tab_kpirule)
{
	m_tabKpiRule = tab_kpirule;
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

	std::string sql = "SELECT SEQ_ID, KPI_ID, STAT_CYCLE FROM " + m_tabTaskReq + " WHERE TASK_STATUS = '00'";
	//m_pLog->Output("[DB2] Select new task request: %s", sql.c_str());

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

			tr_info.status.clear();
			tr_info.status_desc.clear();
			tr_info.gentime.clear();
			tr_info.finishtime.clear();
			tr_info.desc.clear();
			v_tri.push_back(tr_info);

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_SEL_NEW_TREQ, "[DB2] Select new task request from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskReq.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_tri.swap(vec_trinfo);
}

void TaskDB2::SelectTaskState(TaskState& t_state) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT TASK_STATUS, STATUS_DESC, TASK_DESC FROM " + m_tabTaskReq + " WHERE SEQ_ID = ?";
	//m_pLog->Output("[DB2] Select task state [%d]", t_state.seq_id);

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = t_state.seq_id;
		rs.Execute();

		int count = 0;
		while ( !rs.IsEOF() )
		{
			++count;

			int index = 1;
			t_state.state      = (const char*)rs[index++];
			t_state.state_desc = (const char*)rs[index++];
			t_state.task_desc  = (const char*)rs[index++];

			rs.MoveNext();
		}

		if ( 0 == count )
		{
			throw base::Exception(TDB_ERR_SEL_TASK_STATE, "[DB2] Select task state from table '%s' failed! NO record! [SEQ:%d] [FILE:%s, LINE:%d]", m_tabTaskReq.c_str(), t_state.seq_id, __FILE__, __LINE__);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_SEL_TASK_STATE, "[DB2] Select task state from table '%s' failed! [SEQ:%d] [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskReq.c_str(), t_state.seq_id, ex.what(), __FILE__, __LINE__);
	}
}

void TaskDB2::UpdateTaskRequest(TaskReqInfo& task_req) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabTaskReq + " SET TASK_STATUS = ?, STATUS_DESC = ?, TASK_FINISHTIME = ?, TASK_DESC = ? WHERE SEQ_ID = ?";
	m_pLog->Output("[DB2] Update task request [%d]", task_req.seq_id);

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = task_req.status.c_str();
		rs.Parameter(index++) = task_req.status_desc.c_str();
		rs.Parameter(index++) = task_req.finishtime.c_str();
		rs.Parameter(index++) = task_req.desc.c_str();
		rs.Parameter(index++) = task_req.seq_id;

		rs.Execute();

		Commit();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_UPD_TASK_REQ, "[DB2] Update task request to table '%s' failed! [SEQ:%d, KPI_ID:%s] [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskReq.c_str(), task_req.seq_id, task_req.kpi_id.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void TaskDB2::SelectKpiRule(const std::string& kpi, KpiRuleInfo& kpi_info) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT ETLRULE_ID, ANALYSIS_ID FROM " + m_tabKpiRule + " WHERE KPI_ID = '" + kpi + "'";
	m_pLog->Output("[DB2] Select kpi rule info: %s", sql.c_str());

	kpi_info.kpi_id = kpi;
	kpi_info.etl_id.clear();
	kpi_info.ana_id.clear();

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			kpi_info.etl_id = (const char*)rs[1];
			kpi_info.ana_id = (const char*)rs[2];

			rs.MoveNext();
		}

		if ( kpi_info.etl_id.empty() || kpi_info.ana_id.empty() )
		{
			throw base::Exception(TDB_ERR_SEL_KPI_INFO, "[DB2] Select kpi rule info from table '%s' failed! [KPI_ID:%s, ETL_ID:%s, ANA_ID:%s] [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), kpi.c_str(), kpi_info.etl_id.c_str(), kpi_info.ana_id.c_str(), __FILE__, __LINE__);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_SEL_KPI_INFO, "[DB2] Select kpi rule info from table '%s' failed! [KPI_ID:%s] [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), kpi.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void TaskDB2::UpdateEtlTime(const std::string& etl_id, const std::string& etl_time) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabEtlRule + " SET ETLRULE_TIME = '" + etl_time + "' WHERE ETLRULE_ID = '" + etl_id + "'";
	m_pLog->Output("[DB2] Update etlrule_time to [%s] (etlrule_id:%s)", etl_time.c_str(), etl_id.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		Commit();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_UPD_ETL_TIME, "[DB2] Update etlrule_time from table '%s' failed! [ETLRULE_ID:%s] [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), etl_id.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

