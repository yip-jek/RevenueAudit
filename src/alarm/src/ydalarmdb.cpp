#include "ydalarmdb.h"
#include "exception.h"
#include "log.h"
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

void YDAlarmDB::SetTabSrcData(const std::string& tab_srcdata)
{
	m_tabSrcData = tab_srcdata;
}

void YDAlarmDB::SelectAlarmRequest(std::vector<YDAlarmReq>& vecAlarmReq) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<YDAlarmReq> vecReq;
	std::string sql = "SELECT SEQ, WARNDATE, CITY, CHANNELTYPE, CHANNELNAME, BUSITYPE, ";
	sql += "PAYTYPE, REQTIME, FINISHTIME FROM " + m_tabAlarmRequest + " WHERE STATUS = '0'";

	YDAlarmReq alarm_req;
	alarm_req.SetReqStatus("0");

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			alarm_req.seq          = (int)rs[index++];
			alarm_req.alarm_date   = (const char*)rs[index++];
			alarm_req.region       = (const char*)rs[index++];
			alarm_req.channel_type = (const char*)rs[index++];
			alarm_req.channel_name = (const char*)rs[index++];
			alarm_req.busi_type    = (const char*)rs[index++];
			alarm_req.pay_type     = (const char*)rs[index++];
			alarm_req.req_time     = (const char*)rs[index++];
			alarm_req.finish_time  = (const char*)rs[index++];
			vecReq.push_back(alarm_req);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ALMERR_SEL_ALARM_REQ, "[DB2] Select alarm request from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmRequest.c_str(), ex.what(), __FILE__, __LINE__);
	}

	vecReq.swap(vecAlarmReq);
}

void YDAlarmDB::UpdateAlarmRequest(const YDAlarmReq& alarm_req) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabAlarmRequest + " SET STATUS = ?, FINISHTIME = ? WHERE SEQ = ?";
	const std::string REQ_STATUS = alarm_req.GetReqStatus();
	m_pLog->Output("[DB2] Update alarm request: SEQ=[%d], STATUS=[%s], FINISH_TIME=[%s]", alarm_req.seq, REQ_STATUS.c_str(), alarm_req.finish_time.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = REQ_STATUS.c_str();
		rs.Parameter(index++) = alarm_req.finish_time.c_str();
		rs.Parameter(index++) = alarm_req.seq;
		rs.Execute();

		Commit();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ALMERR_UPD_ALARM_REQ, "[DB2] Update alarm request to table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmRequest.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

