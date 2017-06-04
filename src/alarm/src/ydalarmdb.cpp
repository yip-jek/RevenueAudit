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

void YDAlarmDB::SelectAlarmRequest(std::map<int, YDAlarmReq>& mapAlarmReq) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::map<int, YDAlarmReq> mapReq;
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
			alarm_req.request_time = (const char*)rs[index++];
			alarm_req.finish_time  = (const char*)rs[index++];
			mapReq[alarm_req.seq] = alarm_req;

			rs.MoveNext();
		}

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ALMERR_SEL_ALARM_REQ, "[DB2] Select alarm request from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmRequest.c_str(), ex.what(), __FILE__, __LINE__);
	}

	mapReq.swap(mapAlarmReq);
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

void YDAlarmDB::SelectAlarmThreshold(std::vector<YDAlarmThreshold>& vecAlarmThres) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT SEQ, CITY, CHANNELTYPE, CHANNELNAME, RESPONSER, CALLNO";
	sql += ", PAYTYPE, THRESOLD, OFFSET, TEMPLATE FROM " + m_tabAlarmThreshold;

	YDAlarmThreshold alarm_thres;
	std::vector<YDAlarmThreshold> vecThres;

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			alarm_thres.seq          = (int)rs[index++];
			alarm_thres.region       = (const char*)rs[index++];
			alarm_thres.channel_type = (const char*)rs[index++];
			alarm_thres.channel_name = (const char*)rs[index++];
			alarm_thres.responser    = (const char*)rs[index++];
			alarm_thres.call_no      = (const char*)rs[index++];
			alarm_thres.pay_type     = (const char*)rs[index++];
			alarm_thres.threshold    = (double)rs[index++];
			alarm_thres.offset       = (int)rs[index++];
			alarm_thres.msg_template = (const char*)rs[index++];
			vecThres.push_back(alarm_thres);

			rs.MoveNext();
		}

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ALMERR_UPD_ALARM_THRES, "[DB2] Select alarm threshold from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmThreshold.c_str(), ex.what(), __FILE__, __LINE__);
	}

	vecThres.swap(vecAlarmThres);
}

void YDAlarmDB::SelectAlarmData(int seq, const std::string& condition, std::vector<YDAlarmData>& vecData) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT DATE, MANAGELEVEL, CHANNELATTR, CHANNELNAME, BUSSORT, PAYCODE, ";
	sql += "(SUM(receiveFee) - SUM(realFee) - SUM(preferFee)) as FEE FROM " + m_tabSrcData;
	sql += " WHERE " + condition + " GROUP BY DATE, MANAGELEVEL, CHANNELATTR, CHANNELNAME, BUSSORT, PAYCODE";

	YDAlarmData alarm_dat;
	alarm_dat.seq = seq;

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			alarm_dat.alarm_date   = (const char*)rs[index++];
			alarm_dat.manage_level = (const char*)rs[index++];
			alarm_dat.channel_attr = (const char*)rs[index++];
			alarm_dat.channel_name = (const char*)rs[index++];
			alarm_dat.bus_sort     = (const char*)rs[index++];
			alarm_dat.pay_code     = (const char*)rs[index++];
			alarm_dat.arrears      = (double)rs[index++];
			vecData.push_back(alarm_dat);

			rs.MoveNext();
		}

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ALMERR_SEL_ALARM_SRCDAT, "[DB2] Select alarm source data from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabSrcData.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YDAlarmDB::InsertAlarmInfo(const YDAlarmInfo& alarm_info) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "INSERT INTO " + m_tabAlarmInfo + "(WARNDATE, CITY, CHANNELTYPE, CHANNELNAME, ";
	sql += "BUSITYPE, PAYTYPE, RESPONSER, GENDATE, PLANDATE) values(?, ?, ?, ?, ?, ?, ?, ?, ?)";

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = alarm_info.alarm_date.c_str();
		rs.Parameter(index++) = alarm_info.region.c_str();
		rs.Parameter(index++) = alarm_info.channel_type.c_str();
		rs.Parameter(index++) = alarm_info.channel_name.c_str();
		rs.Parameter(index++) = alarm_info.busi_type.c_str();
		rs.Parameter(index++) = alarm_info.pay_type.c_str();
		rs.Parameter(index++) = alarm_info.responser.c_str();
		rs.Parameter(index++) = alarm_info.generate_time.c_str();
		rs.Parameter(index++) = alarm_info.plan_time.c_str();
		rs.Execute();

		Commit();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ALMERR_INS_ALARM_INFO, "[DB2] Insert alarm info to table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmInfo.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

