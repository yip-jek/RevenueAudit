#include "yctaskdb2.h"
#include "log.h"


YCTaskDB2::YCTaskDB2(const DBInfo& db_info)
:base::BaseDB2(db_info.db_inst, db_info.db_user, db_info.db_pwd)
{
}

YCTaskDB2::~YCTaskDB2()
{
	Disconnect();
}

void YCTaskDB2::SetTabTaskRequest(const std::string& tab_taskreq)
{
	m_tabTaskReq = tab_taskreq;
}

void YCTaskDB2::SetTabKpiRule(const std::string& tab_kpirule)
{
	m_tabKpiRule = tab_kpirule;
}

void YCTaskDB2::SetTabEtlRule(const std::string& tab_etlrule)
{
	m_tabEtlRule = tab_etlrule;
}

void YCTaskDB2::SetTabYLStatus(const std::string& tab_ylStatus)
{
	m_tabYLStatus = tab_ylStatus;
}

void YCTaskDB2::SetTabReportKpiRela(const std::string &tabCfgPfLfRela){

    m_tabCfgPfLfRela = tabCfgPfLfRela;
}


bool YCTaskDB2::IsTableExists(const std::string& tab_name)
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

void YCTaskDB2::SelectNewTaskRequest(std::vector<TaskReqInfo>& vec_trinfo)
{
    std::string datestr_yyyymm = "yyyyMM";
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	TaskReqInfo tr_info;
	std::vector<TaskReqInfo> v_tri;

	std::string sql = "SELECT SEQ_ID, KPI_ID, TASK_CITY, STAT_CYCLE FROM " + m_tabTaskReq + " WHERE TASK_STATUS = '00'";
	//m_pLog->Output("[DB2] Select new task request: %s", sql.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			tr_info.seq_id     = (int)rs[index++];
			tr_info.kpi_id     = (const char*)rs[index++];
			tr_info.stat_city  = (const char*)rs[index++];
			tr_info.stat_cycle = (const char*)rs[index++];
            if(datestr_yyyymm.length() == tr_info.stat_cycle.length()) tr_info.stat_cycle.append("01");

			tr_info.status.clear();
			tr_info.status_desc.clear();
			tr_info.gentime.clear();
			tr_info.finishtime.clear();
			tr_info.desc.clear();
			v_tri.push_back(tr_info);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_SEL_NEW_TREQ, "[DB2] Select new task request from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskReq.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_tri.swap(vec_trinfo);
}

void YCTaskDB2::SelectTaskState(TaskState& t_state)
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
		rs.Close();

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

void YCTaskDB2::UpdateTaskRequest(TaskReqInfo& task_req)
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

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_UPD_TASK_REQ, "[DB2] Update task request to table '%s' failed! [SEQ:%d, KPI_ID:%s] [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskReq.c_str(), task_req.seq_id, task_req.kpi_id.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCTaskDB2::SelectKpiRule(const std::string& kpi, KpiRuleInfo& kpi_info)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT ETLRULE_ID, ANALYSIS_ID, KPI_TYPE FROM " + m_tabKpiRule + " WHERE KPI_ID = '" + kpi + "'";
	m_pLog->Output("[DB2] Select kpi rule info: %s", sql.c_str());

	kpi_info.kpi_id = kpi;
	kpi_info.etl_id.clear();
	kpi_info.ana_id.clear();
    kpi_info.kpi_type.clear();

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			kpi_info.etl_id = (const char*)rs[1];
			kpi_info.ana_id = (const char*)rs[2];
            kpi_info.kpi_type = (const char*)rs[3];

			rs.MoveNext();
		}
		rs.Close();

		if ( kpi_info.etl_id.empty() || kpi_info.ana_id.empty() || kpi_info.kpi_type.empty())
		{
			throw base::Exception(TDB_ERR_SEL_KPI_INFO, "[DB2] Select kpi rule info from table '%s' failed: NO result! [KPI_ID:%s, ETL_ID:%s, ANA_ID:%s] [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), kpi.c_str(), kpi_info.etl_id.c_str(), kpi_info.ana_id.c_str(), __FILE__, __LINE__);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_SEL_KPI_INFO, "[DB2] Select kpi rule info from table '%s' failed! [KPI_ID:%s] [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), kpi.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCTaskDB2::UpdateEtlTime(const std::string& etl_id, const std::string& etl_time)
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

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_UPD_ETL_TIME, "[DB2] Update etlrule_time from table '%s' failed! [ETLRULE_ID:%s] [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), etl_id.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

int YCTaskDB2::CountAllSubmitStatu(TaskReqInfo& ref_taskreq)
{
    int i_SubmitStatuCount = 0;
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT Count(1) FROM   " + m_tabYLStatus + " A, (select tab1.lf_reportname,tab1.pf_statid from "  +  m_tabCfgPfLfRela + " tab1  WHERE  tab1.pf_statid = ? ) B WHERE A.REPORTNAME = B.lf_reportname AND A.BILLCYC = ? AND A.STATUS = '02' AND A.TYPE = '03' " ;    //省业财地市提交请求

	try
	{
        int index = 1;
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
        rs.Parameter(index++) = ref_taskreq.kpi_id.c_str();
        rs.Parameter(index++) = ref_taskreq.stat_cycle.substr(0,6).c_str();

		rs.Execute();

		while ( !rs.IsEOF() )
		{
            i_SubmitStatuCount = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_SEL_NEW_TREQ, "[DB2] Select COUNT from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabYLStatus.c_str(), ex.what(), __FILE__, __LINE__);
	}

    return i_SubmitStatuCount;
}

void YCTaskDB2::SelectUndoneTaskRequest(std::vector<TaskReqInfo>& vec_Undone,const std::string& FinalStage)
{
    std::string datestr_yyyymm = "yyyyMM";
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	TaskReqInfo tr_info;
	std::vector<TaskReqInfo> v_tri;

	std::string sql = "SELECT SEQ_ID, KPI_ID, TASK_CITY, STAT_CYCLE FROM " + m_tabTaskReq + " WHERE TASK_STATUS not in (" + FinalStage + ")";

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			tr_info.seq_id     = (int)rs[index++];
			tr_info.kpi_id     = (const char*)rs[index++];
			tr_info.stat_city  = (const char*)rs[index++];
			tr_info.stat_cycle = (const char*)rs[index++];
            if(datestr_yyyymm.length() == tr_info.stat_cycle.length()) tr_info.stat_cycle.append("01");

			tr_info.status.clear();
			tr_info.status_desc.clear();
			tr_info.gentime.clear();
			tr_info.finishtime.clear();
			tr_info.desc.clear();
			v_tri.push_back(tr_info);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_SEL_NEW_TREQ, "[DB2] Select Undone request from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskReq.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_tri.swap(vec_Undone);
}

