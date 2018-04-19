#include "ycanadb2.h"
#include "log.h"
#include "pubstr.h"
#include "anaerror.h"
#include "anadbinfo.h"
#include "ycresult_xqb.h"


YCAnaDB2::YCAnaDB2(const std::string& db_name, const std::string& user, const std::string& pwd)
:CAnaDB2(db_name, user, pwd)
{
}

YCAnaDB2::~YCAnaDB2()
{
}

void YCAnaDB2::SetTabYCTaskReq(const std::string& t_yc_taskreq)
{
	m_tabYCTaskReq = t_yc_taskreq;
}

void YCAnaDB2::SetTabYCStatRule(const std::string& t_statrule)
{
	m_tabYCStatRule = t_statrule;
}

void YCAnaDB2::SetTabYCStatLog(const std::string& t_statlog)
{
	m_tabStatLog = t_statlog;
}

void YCAnaDB2::SetTabYCReportStat(const std::string& t_reportstat)
{
	m_tabReportStat = t_reportstat;
}

void YCAnaDB2::SetTabYCProcessLog(const std::string& t_processlog)
{
	m_tabProcessLog = t_processlog;
}

void YCAnaDB2::SelectYCTaskReq(YCTaskReq& task_req) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT TASK_STATUS, STATUS_DESC, STAT_CYCLE, TASK_CITY, TASK_DESC";
	sql += ", ACTOR, OPERATOR, REQ_TYPE FROM " + m_tabYCTaskReq + " WHERE SEQ_ID = ?";
	m_pLog->Output("[DB2] Select task request from table [%s]: SEQ=[%d]", m_tabYCTaskReq.c_str(), task_req.seq);

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = task_req.seq;
		rs.Execute();

		int counter = 0;
		while ( !rs.IsEOF() )
		{
			++counter;

			int index = 1;
			task_req.state      = (const char*)rs[index++];
			task_req.state_desc = (const char*)rs[index++];
			task_req.stat_cycle = (const char*)rs[index++];
			task_req.task_city  = (const char*)rs[index++];
			task_req.task_desc  = (const char*)rs[index++];
			task_req.actor      = (const char*)rs[index++];
			task_req.oper       = (const char*)rs[index++];
			task_req.req_type   = (const char*)rs[index++];

			rs.MoveNext();
		}
		rs.Close();

		if ( 0 == counter )
		{
			throw base::Exception(ANAERR_SEL_YC_TASK_CITY, "[DB2] Select task request from table '%s' failed! NO record of seq: [%d] [FILE:%s, LINE:%d]", m_tabYCTaskReq.c_str(), task_req.seq, __FILE__, __LINE__);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_YC_TASK_CITY, "[DB2] Select task request from table '%s' failed! (SEQ:%d) [CDBException] %s [FILE:%s, LINE:%d]", m_tabYCTaskReq.c_str(), task_req.seq, ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::UpdateYCTaskReq(const YCTaskReq& t_req) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabYCTaskReq + " SET TASK_STATUS = ?, STATUS_DESC = ?, TASK_DESC = ?";
	if ( t_req.task_batch < 0 )		// 批次无效
	{
		// 批次无效时，不更新批次
		//sql += ", TASK_NUM = NULL WHERE SEQ_ID = ?";
		sql += " WHERE SEQ_ID = ?";
	}
	else	// 批次有效
	{
		sql += ", TASK_NUM = ? WHERE SEQ_ID = ?";
	}
	m_pLog->Output("[DB2] Update task request state: %s", t_req.LogPrintInfo().c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = t_req.state.c_str();
		rs.Parameter(index++) = t_req.state_desc.c_str();
		rs.Parameter(index++) = t_req.task_desc.c_str();

		if ( t_req.task_batch >= 0 )	// 批次有效
		{
			rs.Parameter(index++) = t_req.task_batch;
		}

		rs.Parameter(index++) = t_req.seq;
		rs.Execute();

		Commit();
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_UPD_YC_TASK_REQ, "[DB2] Update task request state to table '%s' failed! [SEQ:%d] [CDBException] %s [FILE:%s, LINE:%d]", m_tabYCTaskReq.c_str(), t_req.seq, ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::SelectYCStatRule(const std::string& kpi_id, std::vector<YCStatInfo>& vec_ycsi) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	YCStatInfo yc_si;
	std::vector<YCStatInfo> v_yc_si;

	try
	{
		std::string sql = "select STAT_ID, STAT_NAME, STATDIM_ID, STAT_PRIORITY, STAT_SQL, STAT_REPORT from ";
		sql += m_tabYCStatRule + " where STAT_ID = '" + kpi_id + "'";
		m_pLog->Output("[DB2] Select table [%s]: %s", m_tabYCStatRule.c_str(), sql.c_str());

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			yc_si.stat_id       = (const char*)rs[index++];
			yc_si.stat_name     = (const char*)rs[index++];
			yc_si.SetDim((const char*)rs[index++]);
			yc_si.stat_priority = (const char*)rs[index++];
			yc_si.stat_sql      = (const char*)rs[index++];
			yc_si.stat_report   = (const char*)rs[index++];

			v_yc_si.push_back(yc_si);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_YC_STATRULE, "[DB2] Select table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabYCStatRule.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_yc_si.swap(vec_ycsi);
	m_pLog->Output("[DB2] Select YCRA stat_rule successfully! Record(s): %lu", vec_ycsi.size());
}

void YCAnaDB2::SelectHDBMaxBatch(const std::string& tab_hdb, YCHDBBatch& hd_batch) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	// STAT_DATE 账期时间兼容格式：YYYYMMDD 与 YYYYMM
	std::string sql = "SELECT NVL(MAX(DECIMAL(STAT_NUM,12,0)), 0) FROM " + tab_hdb;
	sql += " WHERE STAT_REPORT = ? AND STAT_ID = ? AND STAT_DATE like '" + hd_batch.stat_date + "%' AND STAT_CITY = ?";
	m_pLog->Output("[DB2] Select max batch from HDB table: %s [REPORT:%s, STAT_ID:%s, DATE:%s, CITY:%s]", tab_hdb.c_str(), hd_batch.stat_report.c_str(), hd_batch.stat_id.c_str(), hd_batch.stat_date.c_str(), hd_batch.stat_city.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = hd_batch.stat_report.c_str();
		rs.Parameter(index++) = hd_batch.stat_id.c_str();
		//rs.Parameter(index++) = hd_batch.stat_date.c_str();
		rs.Parameter(index++) = hd_batch.stat_city.c_str();
		rs.Execute();

		int counter = 0;
		while ( !rs.IsEOF() )
		{
			++counter;
			hd_batch.stat_batch = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_HDB_MAX_BATCH, "[DB2] Select max batch from HDB table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_hdb.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::SelectXQBMaxBatch(const std::string& tab_xqb, YCXQBBatch& xq_batch) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT NVL(MAX(DECIMAL(BUSIVERSION,12,0)), 0) FROM " + tab_xqb;
	sql += " WHERE BILLCYC = ? AND CITY = ? AND TYPE = ?";
	m_pLog->Output("[DB2] Select max batch from XQB table: %s (%s)", tab_xqb.c_str(), xq_batch.LogPrintInfo().c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = xq_batch.bill_cyc.c_str();
		rs.Parameter(index++) = xq_batch.city.c_str();
		rs.Parameter(index++) = xq_batch.type.c_str();
		rs.Execute();

		int counter = 0;
		while ( !rs.IsEOF() )
		{
			++counter;
			xq_batch.busi_batch = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_XQB_MAX_BATCH, "[DB2] Select max batch from XQB table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_xqb.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::UpdateDetailCWResult(const AnaDBInfo& db_info, const std::vector<YCResult_XQB>& vec_result) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	m_pLog->Output("[DB2] UPDATE DETAIL CW: %s", db_info.db2_sql.c_str());

	try
	{
		rs.Prepare(db_info.db2_sql.c_str(), XDBO2::CRecordset::forwardOnly);

		Begin();

		const int VEC_SIZE = vec_result.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			const YCResult_XQB& ref_ycr = vec_result[i];

			int index = 1;
			rs.Parameter(index++) = ref_ycr.GetFactorArea().c_str();	// 区域

			// 一个或多个项目内容
			int max_size = ref_ycr.GetFactorItemSize();
			for ( int i = 0; i < max_size; ++i )
			{
				rs.Parameter(index++) = ref_ycr.GetFactorItem(i).c_str();
			}

			rs.Parameter(index++) = ref_ycr.batch;			// 财务批次

			// 一个或多个 VALUE
			max_size = ref_ycr.GetFactorValueSize();
			for ( int i = 0; i < max_size; ++i )
			{
				rs.Parameter(index++) = ref_ycr.GetFactorValue(i).c_str();
			}

			rs.Parameter(index++) = ref_ycr.bill_cyc.c_str();			// 账期
			rs.Parameter(index++) = ref_ycr.city.c_str();				// 地市
			rs.Parameter(index++) = ref_ycr.type.c_str();				// 类型
			rs.Parameter(index++) = ref_ycr.GetFactorDim().c_str();		// 维度ID
			rs.Parameter(index++) = ref_ycr.batch;						// 业务批次

			//m_pLog->Output("[DB2] UPDATE CW RESULT (%d): %s", (i+1), ref_ycr.LogPrintInfo().c_str());
			rs.Execute();
		}

		Commit();
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_UPD_DETAIL_CW_RESULT, "[DB2] Update detail (CW) result in table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", db_info.target_table.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::UpdateInsertYCDIffSummary(const AnaDBInfo& db_info, const YCResult_HDB& ycr) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	const std::string ETL_DAY = db_info.GetEtlDay();
	const std::string NOW_DAY = db_info.GetNowDay();

	std::string sql = "SELECT COUNT(0) FROM " + db_info.target_table + " WHERE STAT_REPORT = ? ";
	sql += "and STAT_ID = ? and STATDIM_ID = ? and STAT_DATE = ? and STAT_CITY = ?";
	m_pLog->Output("[DB2] Update or insert diff summary to table: [%s]", db_info.target_table.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = ycr.stat_report.c_str();
		rs.Parameter(index++) = ycr.stat_id.c_str();
		rs.Parameter(index++) = ycr.statdim_id.c_str();
		rs.Parameter(index++) = ETL_DAY.c_str();
		rs.Parameter(index++) = ycr.stat_city.c_str();
		rs.Execute();

		int num = 0;
		while ( !rs.IsEOF() )
		{
			num = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();

		m_pLog->Output("[DB2] DIFF SUMMARY DIM_ID=[%s], COUNT:%d", ycr.statdim_id.c_str(), num);
		// 差异汇总维度数据是否存在？
		if ( num > 0 )	// 已存在
		{
			sql  = "UPDATE " + db_info.target_table + " SET STAT_NAME = ?, STAT_VALUE = ?, INTIME = ?, STAT_NUM = ? ";
			sql += "WHERE STAT_REPORT = ? and STAT_ID = ? and STATDIM_ID = ? and STAT_DATE = ? and STAT_CITY = ?";
			m_pLog->Output("[DB2] UPDATE DIFF SUMMARY: %s, ETL_DAY=[%s], NOW_DAY=[%s]", ycr.LogPrintInfo().c_str(), ETL_DAY.c_str(), NOW_DAY.c_str());

			rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

			index = 1;
			rs.Parameter(index++) = ycr.stat_name.c_str();
			rs.Parameter(index++) = ycr.stat_value.c_str();
			rs.Parameter(index++) = NOW_DAY.c_str();
			rs.Parameter(index++) = ycr.stat_batch;
			rs.Parameter(index++) = ycr.stat_report.c_str();
			rs.Parameter(index++) = ycr.stat_id.c_str();
			rs.Parameter(index++) = ycr.statdim_id.c_str();
			rs.Parameter(index++) = ETL_DAY.c_str();
			rs.Parameter(index++) = ycr.stat_city.c_str();
			rs.Execute();

			Commit();
			rs.Close();
		}
		else	// 不存在
		{
			sql  = "INSERT INTO " + db_info.target_table + "(STAT_REPORT, STAT_ID, STAT_NAME, STATDIM_ID";
			sql += ", STAT_VALUE, STAT_DATE, INTIME, STAT_CITY, STAT_NUM) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
			m_pLog->Output("[DB2] INSERT DIFF SUMMARY: %s, ETL_DAY=[%s], NOW_DAY=[%s]", ycr.LogPrintInfo().c_str(), ETL_DAY.c_str(), NOW_DAY.c_str());

			rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

			index = 1;
			rs.Parameter(index++) = ycr.stat_report.c_str();
			rs.Parameter(index++) = ycr.stat_id.c_str();
			rs.Parameter(index++) = ycr.stat_name.c_str();
			rs.Parameter(index++) = ycr.statdim_id.c_str();
			rs.Parameter(index++) = ycr.stat_value.c_str();
			rs.Parameter(index++) = ETL_DAY.c_str();
			rs.Parameter(index++) = NOW_DAY.c_str();
			rs.Parameter(index++) = ycr.stat_city.c_str();
			rs.Parameter(index++) = ycr.stat_batch;
			rs.Execute();

			Commit();
			rs.Close();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_UPD_INS_DIFFSUMMARY, "[DB2] Update or insert diff summary result in table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", db_info.target_table.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::SelectYCSrcMaxBatch(YCSrcInfo& yc_info) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql;
	base::PubStr::SetFormatString(sql, "select max(decimal(%s,12,0)) from %s where %s like '%s%%' and %s = '%s'", yc_info.field_batch.c_str(), yc_info.src_tab.c_str(), yc_info.field_period.c_str(), yc_info.period.c_str(), yc_info.field_city.c_str(), yc_info.city.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		int counter = 0;
		while ( !rs.IsEOF() )
		{
			++counter;
			yc_info.batch = (int)rs[1];

			rs.MoveNext();
		}

		rs.Close();

		if ( 0 == counter )
		{
			throw base::Exception(ANAERR_SEL_SRC_MAX_BATCH, "[DB2] Select YC source max batch from table '%s' failed! NO Record! [FILE:%s, LINE:%d]", yc_info.src_tab.c_str(), __FILE__, __LINE__);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_SRC_MAX_BATCH, "[DB2] Select YC source max batch from table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", yc_info.src_tab.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::InsertYCStatLog(const YCStatLog& stat_log) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "insert into " + m_tabStatLog + "(STAT_REPORT, STAT_NUM, STAT_DATASOURCE";
	sql += ", STAT_CITY, STAT_CYCLE, STAT_TIME) values(?, ?, ?, ?, ?, ?)";
	m_pLog->Output("[DB2] Insert statistics log to table: [%s]", m_tabStatLog.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		Begin();

		int index = 1;
		rs.Parameter(index++) = stat_log.stat_report.c_str();
		rs.Parameter(index++) = stat_log.stat_batch;
		rs.Parameter(index++) = stat_log.stat_datasource.c_str();
		rs.Parameter(index++) = stat_log.stat_city.c_str();
		rs.Parameter(index++) = stat_log.stat_cycle.c_str();
		rs.Parameter(index++) = stat_log.stat_time.c_str();
		rs.Execute();

		Commit();
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_INS_YC_STAT_LOG, "[DB2] Insert YC stat log to table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabStatLog.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::UpdateInsertReportState(const YCReportState& report_state) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	// 忽略角色（用户）
	//std::string sql = "SELECT COUNT(0) FROM " + m_tabReportStat + " WHERE REPORTNAME = ? AND BILLCYC = ? AND CITY = ? AND ACTOR = ?";
	std::string sql = "SELECT COUNT(0) FROM " + m_tabReportStat + " WHERE REPORTNAME = ? AND BILLCYC = ? AND CITY = ?";
	m_pLog->Output("[DB2] Update or insert report state to table: [%s]", m_tabReportStat.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = report_state.report_id.c_str();
		rs.Parameter(index++) = report_state.bill_cyc.c_str();
		rs.Parameter(index++) = report_state.city.c_str();
		//rs.Parameter(index++) = report_state.actor.c_str();		// 忽略角色（用户）
		rs.Execute();

		int record_count = 0;
		while ( !rs.IsEOF() )
		{
			record_count = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();

		m_pLog->Output("[DB2] Record count in report state: [%d]", record_count);
		if ( record_count > 0 )		// 已存在
		{
			// 角色（用户）也一并更新
			sql  = "UPDATE " + m_tabReportStat + " SET ACTOR = ?, STATUS = ?, TYPE = ?";
			sql += " WHERE REPORTNAME = ? AND BILLCYC = ? AND CITY = ?";

			m_pLog->Output("[DB2] UPDATE REPORT STATE: %s", report_state.LogPrintInfo().c_str());
			rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

			index = 1;
			rs.Parameter(index++) = report_state.actor.c_str();
			rs.Parameter(index++) = report_state.status.c_str();
			rs.Parameter(index++) = report_state.type.c_str();
			rs.Parameter(index++) = report_state.report_id.c_str();
			rs.Parameter(index++) = report_state.bill_cyc.c_str();
			rs.Parameter(index++) = report_state.city.c_str();
			rs.Execute();

			Commit();
			rs.Close();
		}
		else	// 不存在
		{
			sql = "INSERT INTO " + m_tabReportStat + "(REPORTNAME, BILLCYC, CITY, STATUS, TYPE, ACTOR) VALUES(?, ?, ?, ?, ?, ?)";

			m_pLog->Output("[DB2] INSERT REPORT STATE: %s", report_state.LogPrintInfo().c_str());
			rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

			index = 1;
			rs.Parameter(index++) = report_state.report_id.c_str();
			rs.Parameter(index++) = report_state.bill_cyc.c_str();
			rs.Parameter(index++) = report_state.city.c_str();
			rs.Parameter(index++) = report_state.status.c_str();
			rs.Parameter(index++) = report_state.type.c_str();
			rs.Parameter(index++) = report_state.actor.c_str();
			rs.Execute();

			Commit();
			rs.Close();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_UPD_INS_REPORTSTATE, "[DB2] Update or insert report state to table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabReportStat.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::InsertProcessLog(const YCProcessLog& proc_log) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "INSERT INTO " + m_tabProcessLog + "(REPORTNAME, BILLCYC, CITY, STATUS, ";
	sql += "TYPE, ACTOR, OPERATOR, VERSION, UPTIME) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
	m_pLog->Output("[DB2] INSERT PROCESS LOG: %s", proc_log.LogPrintInfo().c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = proc_log.report_id.c_str();
		rs.Parameter(index++) = proc_log.bill_cyc.c_str();
		rs.Parameter(index++) = proc_log.city.c_str();
		rs.Parameter(index++) = proc_log.status.c_str();
		rs.Parameter(index++) = proc_log.type.c_str();
		rs.Parameter(index++) = proc_log.actor.c_str();
		rs.Parameter(index++) = proc_log.oper.c_str();
		rs.Parameter(index++) = proc_log.version;
		rs.Parameter(index++) = proc_log.uptime.c_str();
		rs.Execute();

		Commit();
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_INS_PROCESSLOG, "[DB2] Insert process log to table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabProcessLog.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void YCAnaDB2::UpdateLastBatchManualData(const std::string& tab, const UpdateFields_YW& upd_fld, std::vector<std::vector<std::string> >& v2_data) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string str_tmp_s;
	std::string str_tmp_u;

	// DIM 更新字段
	const int DIM_FLD_SIZE = upd_fld.dim_upd_fields.size();
	for ( int i = 0; i < DIM_FLD_SIZE; ++i )
	{
		if ( i > 0 )
		{
			str_tmp_s += ", " + upd_fld.dim_upd_fields[i];
			str_tmp_u += ", " + upd_fld.dim_upd_fields[i] + " = ?";
		}
		else
		{
			str_tmp_s += upd_fld.dim_upd_fields[i];
			str_tmp_u += upd_fld.dim_upd_fields[i] + " = ?";
		}
	}

	// VAL 更新字段
	const int VAL_FLD_SIZE = upd_fld.val_upd_fields.size();
	for ( int i = 0; i < VAL_FLD_SIZE; ++i )
	{
		if ( str_tmp_u.empty() )
		{
			str_tmp_s += upd_fld.val_upd_fields[i];
			str_tmp_u += upd_fld.val_upd_fields[i] + " = ?";
		}
		else
		{
			str_tmp_s += ", " + upd_fld.val_upd_fields[i];
			str_tmp_u += ", " + upd_fld.val_upd_fields[i] + " = ?";
		}
	}

	std::string sql_sel = "SELECT " + str_tmp_s;
	sql_sel += " FROM " + tab + " WHERE " + upd_fld.fld_billcyc + " = ? and " + upd_fld.fld_city;
	sql_sel += " = ? and decimal(" + upd_fld.fld_batch + ",12,0) = ? and " + upd_fld.fld_dim + " = ?";
	m_pLog->Output("[DB2] Select last batch manual data: %s", sql_sel.c_str());

	std::string sql_upd = "UPDATE " + tab + " SET " + str_tmp_u;
	sql_upd += " WHERE " + upd_fld.fld_billcyc + " = ? and " + upd_fld.fld_city;
	sql_upd += " = ? and decimal(" + upd_fld.fld_batch + ",12,0) = ? and " + upd_fld.fld_dim + " = ?";
	m_pLog->Output("[DB2] Update manual data in current batch: %s", sql_upd.c_str());

	int upd_count = 0;
	try
	{
		std::vector<std::string> vec_manual;
		const int VEC2_SIZE = v2_data.size();
		for ( int i = 0; i < VEC2_SIZE; ++i )
		{
			std::vector<std::string>& ref_vec = v2_data[i];

			rs.Prepare(sql_sel.c_str(), XDBO2::CRecordset::forwardOnly);

			int index = 1;
			rs.Parameter(index++) = ref_vec[0].c_str();
			rs.Parameter(index++) = ref_vec[1].c_str();
			rs.Parameter(index++) = upd_fld.last_batch;
			rs.Parameter(index++) = ref_vec[YCResult_XQB::S_PUBLIC_MEMBERS].c_str();
			rs.Execute();

			std::vector<std::string>().swap(vec_manual);
			while ( !rs.IsEOF() )
			{
				// DIM 字段值
				for ( int j = 0; j < DIM_FLD_SIZE; ++j )
				{
					vec_manual.push_back((const char*)rs[j+1]);
				}

				// VAL 字段值
				for ( int k = 0; k < VAL_FLD_SIZE; ++k )
				{
					str_tmp_s = (const char*)rs[DIM_FLD_SIZE+k+1];
					if ( base::PubStr::TrimB(str_tmp_s).empty() )	// VAL 字段值为空
					{
						// 设置默认值：0
						str_tmp_s = "0";
					}

					vec_manual.push_back(str_tmp_s);
				}

				rs.MoveNext();
			}
			rs.Close();

			// 不存在满足条件的手工填列数
			if ( vec_manual.empty() )
			{
				m_pLog->Output("[DB2] <WARNING> NO such manual data record: BILL_CYC=[%s], CITY=[%s], BATCH=[%d], DIM=[%s]", ref_vec[0].c_str(), ref_vec[1].c_str(), upd_fld.last_batch, ref_vec[YCResult_XQB::S_PUBLIC_MEMBERS].c_str());
				continue;
			}

			rs.Prepare(sql_upd.c_str(), XDBO2::CRecordset::forwardOnly);

			index = 1;
			for ( int l = 0; l < (DIM_FLD_SIZE + VAL_FLD_SIZE); ++l )
			{
				rs.Parameter(index++) = vec_manual[l].c_str();
			}

			rs.Parameter(index++) = ref_vec[0].c_str();
			rs.Parameter(index++) = ref_vec[1].c_str();
			rs.Parameter(index++) = upd_fld.last_batch + 1;
			rs.Parameter(index++) = ref_vec[YCResult_XQB::S_PUBLIC_MEMBERS].c_str();
			rs.Execute();

			Commit();
			rs.Close();

			++upd_count;
		}

		m_pLog->Output("[DB2] Select last batch: [%d], update current batch: [%d]", VEC2_SIZE, upd_count);
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_UPD_LAST_BATCH_MANUAL, "[DB2] Update last batch manual data to table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", tab.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

