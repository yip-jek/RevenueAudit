#include "ydalarmdb.h"
#include "dbinfo.h"

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

