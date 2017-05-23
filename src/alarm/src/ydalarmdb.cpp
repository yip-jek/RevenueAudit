#include "ydalarmdb.h"
#include "exception.h"
#include "alarmerror.h"
#include "dbinfo.h"
#include "ydstruct.h"

YDAlarmDB::YDAlarmDB(const DBInfo& db_info)
:base::BaseDB2(db_info.db_inst, db_info.db_user, db_info.db_pwd)
{
}

YDAlarmDB::~YDAlarmDB()
{
	Disconnect();
}

void YDAlarmDB::SetTabAlarmRequest(const std::string& tab_alarmreq)
{
	m_tabAlarmRequest = tab_alarmreq;
}

void YDAlarmDB::SetTabAlarmThreshold(const std::string& tab_alarmthreshold)
{
	m_tabAlarmThreshold = tab_alarmthreshold;
}

void YDAlarmDB::SetTabAlarmInfo(const std::string& tab_alarminfo)
{
	m_tabAlarmInfo = tab_alarminfo;
}

void YDAlarmDB::SelectAlarmRequest(std::vector<YDAlarmReq>& vecAlarmReq) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<YDAlarmReq> vecReq;
	std::string sql = "select SEQ, WARNDATE, CITY, CHANNELTYPE, CHANNELNAME, BUSITYPE, ";
	sql += "PAYTYPE, REQTIME from " + m_tabAlarmRequest + " where STATUS = '0'";
	try
	{
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ALMERR_SEL_ALARM_REQ, "[DB2] Select alarm request from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmRequest.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

