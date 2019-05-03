#include "ydanadb2.h"
#include "log.h"
#include "anaerror.h"


YDAnaDB2::YDAnaDB2(const std::string& db_name, const std::string& user, const std::string& pwd)
:CAnaDB2(db_name, user, pwd)
{
}

YDAnaDB2::~YDAnaDB2()
{
}

void YDAnaDB2::SetTabTaskScheLog(const std::string& t_tslog)
{
	m_tabTaskScheLog = t_tslog;
}

void YDAnaDB2::SetTabAlarmRequest(const std::string& t_alarmreq)
{
	m_tabAlarmRequest = t_alarmreq;
}

void YDAnaDB2::UpdateTaskScheLogState(int log, const std::string& end_time, const std::string& state, const std::string& state_desc, const std::string& remark)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabTaskScheLog + " SET END_TIME = ?, ";
	sql += "TASK_STATE = ?, TASK_STATE_DESC = ?, REMARKS = ? WHERE LOG_ID = ?"; 
	m_pLog->Output("[DB2] Update task schedule log: LOG=[%d], END_TIME=[%s], TASK_STATE=[%s], TASK_STATE_DESC=[%s], REMARK=[%s]", log, end_time.c_str(), state.c_str(), state_desc.c_str(), remark.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = end_time.c_str();
		rs.Parameter(index++) = state.c_str();
		rs.Parameter(index++) = state_desc.c_str();
		rs.Parameter(index++) = remark.c_str();
		rs.Parameter(index++) = log;
		rs.Execute();

		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_UPD_TSLOG_STATE, "[DB2] Update state of task schedule log in table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabTaskScheLog.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YDAnaDB2::SelectAllCity(std::vector<std::string>& vec_city)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<std::string> v_city;
	std::string sql = "SELECT DISTINCT CITYID FROM " + m_tabDictCity;

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			v_city.push_back((const char*)rs[1]);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_ALL_CITY, "[DB2] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDictCity.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_city.swap(vec_city);
}

