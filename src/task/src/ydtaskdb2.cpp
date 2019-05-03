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

void YDTaskDB2::SetTabTaskScheLog(const std::string& tab_taskschelog)
{
	m_tabTaskScheLog = tab_taskschelog;
}

void YDTaskDB2::SetTabKpiRule(const std::string& tab_kpirule)
{
	m_tabKpiRule = tab_kpirule;
}

void YDTaskDB2::SetTabEtlRule(const std::string& tab_etlrule)
{
	m_tabEtlRule = tab_etlrule;
}

bool YDTaskDB2::IsTableExists(const std::string& tab_name)
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

void YDTaskDB2::GetTaskSchedule(std::map<int, TaskSchedule>& m_tasksche)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT SEQ_ID, TASK_TYPE, KPI_ID, TASK_CYCLE, ETL_TIME, ";
	sql += "EXPIRY_DATE_START, EXPIRY_DATE_END FROM " + m_tabTaskSche + " WHERE ACTIVATE = '1'";

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
			task_sche.task_type         = (const char*)rs[index++];
			task_sche.kpi_id            = (const char*)rs[index++];
			task_sche.task_cycle        = (const char*)rs[index++];
			task_sche.etl_time          = (const char*)rs[index++];
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

bool YDTaskDB2::IsTaskScheExist(int id)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT COUNT(0) FROM " + m_tabTaskSche + " WHERE SEQ_ID = ?";

	int ct = 0;
	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = id;
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			ct = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_IS_TASKCHE_EXIST, "[DB2] Check task schedule whether exist or not in table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskSche.c_str(), ex.what(), __FILE__, __LINE__);
	}

	return (ct > 0);
}

void YDTaskDB2::UpdateTaskScheTaskTime(int id, const std::string& start_time, const std::string& finish_time)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabTaskSche + " SET TASK_STARTTIME = ?, TASK_FINISHTIME = ? WHERE SEQ_ID = ?";
	m_pLog->Output("[DB2] Update task schedule task time: START_TIME=[%s], FINISH_TIME=[%s] (SEQ:%d)", start_time.c_str(), finish_time.c_str(), id);

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = start_time.c_str();
		rs.Parameter(index++) = finish_time.c_str();
		rs.Parameter(index++) = id;
		rs.Execute();

		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_UPD_TASKSCHE_TIME, "[DB2] Update task time to task schedule table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskSche.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YDTaskDB2::SetTaskScheNotActive(int id)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabTaskSche + " SET ACTIVATE = '0' WHERE SEQ_ID = ?";
	m_pLog->Output("[DB2] Update task schedule not active: SEQ=[%d]", id);

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = id;
		rs.Execute();

		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_SET_TS_NOTACTIVE, "[DB2] Set task schedule not active in table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskSche.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

bool YDTaskDB2::GetKpiRuleSubID(const std::string& kpi_id, std::string& etl_id, std::string& ana_id)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT ETLRULE_ID, ANALYSIS_ID FROM " + m_tabKpiRule + " WHERE KPI_ID = '" + kpi_id + "'";
	m_pLog->Output("[DB2] Get kpi rule sub_id: KPI=[%s]", kpi_id.c_str());

	int ct = 0;
	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			++ct;

			etl_id = (const char*)rs[1];
			ana_id = (const char*)rs[2];

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_GET_KPIRULE_SUBID, "[DB2] Get sub_id from kpi rule table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), ex.what(), __FILE__, __LINE__);
	}

	return (ct > 0);
}

int YDTaskDB2::GetTaskScheLogMaxID()
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT NVL(MAX(LOG_ID), 0) FROM " + m_tabTaskScheLog;
	m_pLog->Output("[DB2] Get MAX(log_id): %s", sql.c_str());

	int max_log_id = 0;
	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			max_log_id = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_GET_TSLOG_MAXID, "[DB2] Get max log_id from task schedule log table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskScheLog.c_str(), ex.what(), __FILE__, __LINE__);
	}

	return max_log_id;
}

void YDTaskDB2::InsertTaskScheLog(const TaskScheLog& ts_log)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "INSERT INTO " + m_tabTaskScheLog;
	sql += "(LOG_ID, KPI_ID, SUB_ID, TASK_ID, TASK_TYPE, ETL_TIME, APP_TYPE, START_TIME, END_TIME";
	sql += ", TASK_STATE, TASK_STATE_DESC, REMARKS) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	m_pLog->Output("[DB2] Insert task schedule log: LOG=[%d], KPI=[%s], SUB_ID=[%s], TASK_ID=[%s], ETL_TIME=[%s], APP_TYPE=[%s], START_TIME=[%s], END_TIME=[%s], TASK_STATE=[%s]", ts_log.log_id, ts_log.kpi_id.c_str(), ts_log.sub_id.c_str(), ts_log.task_id.c_str(), ts_log.etl_time.c_str(), ts_log.app_type.c_str(), ts_log.start_time.c_str(), ts_log.end_time.c_str(), ts_log.task_state.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = ts_log.log_id;
		rs.Parameter(index++) = ts_log.kpi_id.c_str();
		rs.Parameter(index++) = ts_log.sub_id.c_str();
		rs.Parameter(index++) = ts_log.task_id.c_str();
		rs.Parameter(index++) = ts_log.task_type.c_str();
		rs.Parameter(index++) = ts_log.etl_time.c_str();
		rs.Parameter(index++) = ts_log.app_type.c_str();
		rs.Parameter(index++) = ts_log.start_time.c_str();
		rs.Parameter(index++) = ts_log.end_time.c_str();
		rs.Parameter(index++) = ts_log.task_state.c_str();
		rs.Parameter(index++) = ts_log.state_desc.c_str();
		rs.Parameter(index++) = ts_log.remarks.c_str();
		rs.Execute();

		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_INS_TASKSCHELOG, "[DB2] Insert into task schedule log table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskScheLog.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YDTaskDB2::SelectTaskScheLogState(TaskScheLog& ts_log)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT END_TIME, TASK_STATE, TASK_STATE_DESC, REMARKS FROM ";
	sql += m_tabTaskScheLog + " WHERE LOG_ID = ?";

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = ts_log.log_id;
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;
			ts_log.end_time   = (const char*)rs[index++];
			ts_log.task_state = (const char*)rs[index++];
			ts_log.state_desc = (const char*)rs[index++];
			ts_log.remarks    = (const char*)rs[index++];

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_SEL_TSLOG_STATE, "[DB2] Select state from task schedule log table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskScheLog.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YDTaskDB2::UpdateEtlTime(const std::string& etl_id, const std::string& etl_time)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabEtlRule + " SET ETLRULE_TIME = '" + etl_time + "' WHERE ETLRULE_ID = '" + etl_id + "'";
	m_pLog->Output("[DB2] Update etl time: %s (ETL_ID:%s)", etl_time.c_str(), etl_id.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_UPD_ETL_TIME, "[DB2] Update etl time to etl rule table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YDTaskDB2::UpdateTaskScheLog(const TaskScheLog& ts_log)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabTaskScheLog + " SET END_TIME = ?, ";
	sql += "TASK_STATE = ?, TASK_STATE_DESC = ?, REMARKS = ? WHERE LOG_ID = ?"; 
	m_pLog->Output("[DB2] Update task schedule log: LOG=[%d], TASK_ID=[%s], START_TIME=[%s], END_TIME=[%s], TASK_STATE=[%s], TASK_STATE_DESC=[%s]", ts_log.log_id, ts_log.task_id.c_str(), ts_log.start_time.c_str(), ts_log.end_time.c_str(), ts_log.task_state.c_str(), ts_log.state_desc.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = ts_log.end_time.c_str();
		rs.Parameter(index++) = ts_log.task_state.c_str();
		rs.Parameter(index++) = ts_log.state_desc.c_str();
		rs.Parameter(index++) = ts_log.remarks.c_str();
		rs.Parameter(index++) = ts_log.log_id;
		rs.Execute();

		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(TDB_ERR_UPD_TASKSCHELOG, "[DB2] Update task schedule log table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskScheLog.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

