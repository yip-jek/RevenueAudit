#include "canadb2.h"
#include "log.h"
#include "pubstr.h"
#include "anadbinfo.h"
#include "anaerror.h"
#include "uniformcodetransfer.h"


CAnaDB2::CAnaDB2(const std::string& db_name, const std::string& usr, const std::string& pw)
:BaseDB2(db_name, usr, pw)
{
}

CAnaDB2::~CAnaDB2()
{
}

void CAnaDB2::SetTabKpiRule(const std::string& t_kpirule)
{
	m_tabKpiRule = t_kpirule;
}

void CAnaDB2::SetTabKpiColumn(const std::string& t_kpicol)
{
	m_tabKpiColumn = t_kpicol;
}

void CAnaDB2::SetTabDimValue(const std::string& t_dimval)
{
	m_tabDimValue = t_dimval;
}

void CAnaDB2::SetTabEtlRule(const std::string& t_etlrule)
{
	m_tabEtlRule = t_etlrule;
}

void CAnaDB2::SetTabEtlDim(const std::string& t_etldim)
{
	m_tabEtlDim = t_etldim;
}

void CAnaDB2::SetTabEtlVal(const std::string& t_etlval)
{
	m_tabEtlVal = t_etlval;
}

void CAnaDB2::SetTabAnaRule(const std::string& t_anarule)
{
	m_tabAnaRule = t_anarule;
}

void CAnaDB2::SetTabDictChannel(const std::string& t_dictchann)
{
	m_tabDictChannel = t_dictchann;
}

void CAnaDB2::SetTabDictCity(const std::string& t_dictcity)
{
	m_tabDictCity = t_dictcity;
}

void CAnaDB2::SetTabTaskScheLog(const std::string& t_tslog)
{
	m_tabTaskScheLog = t_tslog;
}

void CAnaDB2::SetTabAlarmRequest(const std::string& t_alarmreq)
{
	m_tabAlarmRequest = t_alarmreq;
}

void CAnaDB2::SetTabYCStatRule(const std::string& t_statrule)
{
	m_tabYCStatRule = t_statrule;
}

void CAnaDB2::SetTabYCStatLog(const std::string& t_statlog)
{
	m_tabStatLog = t_statlog;
}

void CAnaDB2::SetTabYCTaskReq(const std::string& t_yc_taskreq)
{
	m_tabYCTaskReq = t_yc_taskreq;
}

void CAnaDB2::SelectYCTaskReqCity(int seq, std::string& city) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT TASK_CITY FROM " + m_tabYCTaskReq + " WHERE SEQ_ID = ?";
	m_pLog->Output("[DB2] Select city from YC task request table: %s (SEQ:%d)", m_tabYCTaskReq.c_str(), seq);

	int counter = 0;
	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = seq;
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			++counter;

			city = (const char*)rs[1];
			rs.MoveNext();
		}

		rs.Close();

		if ( 0 == counter )
		{
			throw base::Exception(ANAERR_SEL_YC_TASK_CITY, "[DB2] Select city from YC task request table '%s' failed! [SEQ:%d] NO Record! [FILE:%s, LINE:%d]", m_tabYCTaskReq.c_str(), seq, __FILE__, __LINE__);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_YC_TASK_CITY, "[DB2] Select city from YC task request table '%s' failed! [SEQ:%d, REC:%d] [CDBException] %s [FILE:%s, LINE:%d]", m_tabYCTaskReq.c_str(), seq, counter, ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::UpdateYCTaskReq(const YCTaskReq& t_req) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabYCTaskReq + " SET TASK_STATUS = ?, STATUS_DESC = ?, TASK_DESC = ?";
	if ( t_req.task_batch < 0 )		// 批次无效
	{
		sql += ", TASK_NUM = NULL WHERE SEQ_ID = ?";
	}
	else	// 批次有效
	{
		sql += ", TASK_NUM = ? WHERE SEQ_ID = ?";
	}
	m_pLog->Output("[DB2] Update task request: STATE=%s, STATE_DESC=%s, TASK_BATCH=%d (SEQ:%d)", t_req.state.c_str(), t_req.state_desc.c_str(), t_req.task_batch, t_req.seq);

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
		throw base::Exception(ANAERR_UPD_YC_TASK_REQ, "[DB2] Update task request to table '%s' failed! [SEQ:%d] [CDBException] %s [FILE:%s, LINE:%d]", m_tabYCTaskReq.c_str(), t_req.seq, ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::SelectStatResultMaxBatch(const std::string& tab_result, YCStatBatch& st_batch) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "SELECT NVL(MAX(STAT_NUM), 0) FROM " + tab_result;
	sql += " WHERE STAT_REPORT = ? AND STAT_ID = ? AND STAT_DATE = ? AND STAT_CITY = ?";
	m_pLog->Output("[DB2] Select max batch from result table: %s [REPORT:%s, STAT_ID:%s, DATE:%s, CITY:%s]", tab_result.c_str(), st_batch.stat_report.c_str(), st_batch.stat_id.c_str(), st_batch.stat_date.c_str(), st_batch.stat_city.c_str());

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = st_batch.stat_report.c_str();
		rs.Parameter(index++) = st_batch.stat_id.c_str();
		rs.Parameter(index++) = st_batch.stat_date.c_str();
		rs.Parameter(index++) = st_batch.stat_city.c_str();

		rs.Execute();

		int counter = 0;
		while ( !rs.IsEOF() )
		{
			++counter;
			st_batch.stat_batch = (int)rs[1];

			rs.MoveNext();
		}

		rs.Close();

		if ( 0 == counter )
		{
			throw base::Exception(ANAERR_SEL_RS_MAX_BATCH, "[DB2] Select max batch from result table '%s' failed! NO Record! [FILE:%s, LINE:%d]", tab_result.c_str(), __FILE__, __LINE__);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_RS_MAX_BATCH, "[DB2] Select max batch from result table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_result.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::SelectSequence(const std::string& seq_name, size_t size, std::vector<std::string>& vec_seq) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "select " + seq_name + ".NEXTVAL from sysibm.sysdummy1";
	m_pLog->Output("[DB2] Select sequence: %s (size: %llu)", sql.c_str(), size);

	std::vector<std::string> v_seq;
	try
	{
		while ( size-- > 0 )
		{
			rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
			rs.Execute();

			while ( !rs.IsEOF() )
			{
				v_seq.push_back((const char*)rs[1]);

				rs.MoveNext();
			}

			rs.Close();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		m_pLog->Output("[DB2] Before CDBException, select sequence '%s' size: %llu", seq_name.c_str(), v_seq.size());

		throw base::Exception(ANAERR_SEL_SEQUENCE, "[DB2] Select sequence '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", seq_name.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_seq.swap(vec_seq);

	m_pLog->Output("[DB2] Select sequence '%s' size: %llu", seq_name.c_str(), vec_seq.size());
}

void CAnaDB2::SelectYCSrcMaxBatch(YCSrcInfo& yc_info) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql;
	base::PubStr::SetFormatString(sql, "select max(%s) from %s where %s = '%s' and %s = '%s'", yc_info.field_batch.c_str(), yc_info.src_tab.c_str(), yc_info.field_period.c_str(), yc_info.period.c_str(), yc_info.field_city.c_str(), yc_info.city.c_str());

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

void CAnaDB2::InsertYCStatLog(const YCStatLog& stat_log) throw(base::Exception)
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

void CAnaDB2::SelectAnaTaskInfo(AnaTaskInfo& info) throw(base::Exception)
{
	// 获取指标规则数据
	SelectKpiRule(info);

	// 获取指标字段数据
	SelectKpiColumn(info);

	// 获取采集规则数据
	const int VEC_SIZE = info.vecEtlRule.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		OneEtlRule& one = info.vecEtlRule[i];

		SelectEtlRule(one);

		// 获取采集维度信息
		SelectEtlDim(one.DimID, one.vecEtlDim, one.vecEtlSingleDim);

		// 获取采集值信息
		SelectEtlVal(one.ValID, one.vecEtlVal);
	}

	// 获取分析规则数据
	SelectAnaRule(info.AnaRule);

	// 获取对比结果字段名称
	const std::string COMPARE_RESULT_DBNAME = GetCompareResultName(info.vecKpiValCol);

	// 获取对比结果描述（从维度取值表中获取）
	SelectCompareResultDesc(info.KpiID, COMPARE_RESULT_DBNAME, info.vecComResDesc);
}

void CAnaDB2::SelectChannelUniformCode(std::vector<ChannelUniformCode>& vec_channunicode) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	ChannelUniformCode chann_unicode;
	std::vector<ChannelUniformCode> v_chann_uc;

	try
	{
		std::string sql = "select channelid, channelalias, channelname, remarks from " + m_tabDictChannel;

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			chann_unicode.ChannelID    = (const char*)rs[index++];
			chann_unicode.ChannelAlias = (const char*)rs[index++];
			chann_unicode.ChannelName  = (const char*)rs[index++];
			chann_unicode.Remarks      = (const char*)rs[index++];

			v_chann_uc.push_back(chann_unicode);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_CHANN_UNICODE, "[DB2] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDictChannel.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s successfully! [Record: %llu]", m_tabDictChannel.c_str(), v_chann_uc.size());

	v_chann_uc.swap(vec_channunicode);
}

void CAnaDB2::SelectCityUniformCode(std::vector<CityUniformCode>& vec_cityunicode) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	CityUniformCode city_unicode;
	std::vector<CityUniformCode> v_city_uc;

	try
	{
		std::string sql = "select cityid, cityalias, cityname, remarks from " + m_tabDictCity;

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			city_unicode.CityID    = (const char*)rs[index++];
			city_unicode.CityAlias = (const char*)rs[index++];
			city_unicode.CityName  = (const char*)rs[index++];
			city_unicode.Remarks   = (const char*)rs[index++];

			v_city_uc.push_back(city_unicode);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_CITY_UNICODE, "[DB2] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDictCity.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s successfully! [Record: %llu]", m_tabDictCity.c_str(), v_city_uc.size());

	v_city_uc.swap(vec_cityunicode);
}

void CAnaDB2::SelectAllCity(std::vector<std::string>& vec_city) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<std::string> v_city;
	std::string sql = "SELECT DISTINCT CITYID FROM " + m_tabDictCity;

	try
	{
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_ALL_CITY, "[DB2] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDictCity.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_city.swap(vec_city);
}

void CAnaDB2::SelectKpiRule(AnaTaskInfo& info) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	int counter = 0;
	std::string str_etlruleid;

	try
	{
		std::string sql = "select DATA_SOURCE, ETLRULE_ID, KPI_CYCLE, RESULT_TYPE, KPI_TABLENAME, ALARM_ID from ";
		sql += m_tabKpiRule + " where KPI_ID = ?";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = info.KpiID.c_str();
		rs.Execute();

		std::string result_type;

		while ( !rs.IsEOF() )
		{
			++counter;

			int index = 1;

			info.DataSrcType   = (const char*)rs[index++];
			str_etlruleid      = (const char*)rs[index++];
			info.KpiCycle      = (const char*)rs[index++];
			result_type        = (const char*)rs[index++];
			info.TableName     = (const char*)rs[index++];
			info.AlarmID       = (const char*)rs[index++];

			if ( !info.SetTableType(result_type) )
			{
				throw base::Exception(ANAERR_SEL_KPI_RULE, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的结果表类型: %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), result_type.c_str(), __FILE__, __LINE__);
			}

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_KPI_RULE, "[DB2] Select %s failed! (KPI_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ANAERR_SEL_KPI_RULE, "[DB2] Select %s failed! No record! (KPI_ID:%s) [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB2] Select %s successfully! (KPI_ID:%s) [Record:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), counter);

	// 采集规则集
	std::vector<std::string> vec_id;
	base::PubStr::Str2StrVector(str_etlruleid, "|", vec_id);

	std::vector<OneEtlRule> vec_etl;
	OneEtlRule one;
	one.KpiID = info.KpiID;
	const int VEC_SIZE = vec_id.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		one.EtlRuleID = vec_id[i];

		vec_etl.push_back(one);
	}
	vec_etl.swap(info.vecEtlRule);
}

void CAnaDB2::SelectKpiColumn(AnaTaskInfo& info) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<KpiColumn> v_dim;
	std::vector<KpiColumn> v_val;
	std::vector<KpiColumn> v_left;
	std::vector<KpiColumn> v_right;

	KpiColumn col;
	col.KpiID = info.KpiID;

	try
	{
		std::string sql = "select COLUMN_TYPE, COLUMN_SEQ, DB_NAME, CN_NAME, DIS_TYPE, EXP_WAY from ";
		sql += m_tabKpiColumn + " where KPI_ID = ? order by COLUMN_TYPE, COLUMN_SEQ asc";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = info.KpiID.c_str();
		rs.Execute();

		std::string col_type;
		std::string dis_type;
		std::string exp_way;

		while ( !rs.IsEOF() )
		{
			int index = 1;

			col_type    = (const char*)rs[index++];
			col.ColSeq  = (int)rs[index++];
			col.DBName  = (const char*)rs[index++];
			col.CNName  = (const char*)rs[index++];
			dis_type    = (const char*)rs[index++];
			exp_way     = (const char*)rs[index++];

			if ( !col.SetDisplayType(dis_type) )
			{
				throw base::Exception(ANAERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的前台显示方式: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), dis_type.c_str(), __FILE__, __LINE__);
			}

			if ( !col.SetColumnType(col_type) )
			{
				throw base::Exception(ANAERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的指标字段类型: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), col_type.c_str(), __FILE__, __LINE__);
			}

			if ( !col.SetExpWayType(exp_way) )
			{
				throw base::Exception(ANAERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的后台表示方式: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), exp_way.c_str(), __FILE__, __LINE__);
			}

			if ( KpiColumn::EWTYPE_SINGLE_LEFT == col.ExpWay )		// 左侧单独显示方式
			{
				v_left.push_back(col);
			}
			else if ( KpiColumn::EWTYPE_SINGLE_RIGHT == col.ExpWay )	// 右侧单独显示方式
			{
				v_right.push_back(col);
			}
			else
			{
				switch ( col.ColType )
				{
				case KpiColumn::CTYPE_DIM:		// 维度
					v_dim.push_back(col);
					break;
				case KpiColumn::CTYPE_VAL:		// 值
					v_val.push_back(col);
					break;
				default:
					throw base::Exception(ANAERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的指标字段类型: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), col_type.c_str(), __FILE__, __LINE__);
				}
			}

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_dim.empty() )
	{
		throw base::Exception(ANAERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 没有维度数据! [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), __FILE__, __LINE__);
	}
	if ( v_val.empty() )
	{
		throw base::Exception(ANAERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 没有值数据! [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s successfully! (KPI_ID:%s) [ETLDIM size:%lu] [ETLVAL size:%lu] [SINGLE_LEFT size:%lu] [SINGLE_RIGHT size:%lu]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), v_dim.size(), v_val.size(), v_left.size(), v_right.size());

	v_dim.swap(info.vecKpiDimCol);
	v_val.swap(info.vecKpiValCol);
	v_left.swap(info.vecLeftKpiCol);
	v_right.swap(info.vecRightKpiCol);
}

void CAnaDB2::SelectDimValue(const std::string& kpi_id, DimValDiffer& differ) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	DimVal d_v;
	d_v.KpiID = kpi_id;

	try
	{
		std::string sql = "select DB_NAME, DIM_VAL, VAL_CNAME from ";
		sql += m_tabDimValue + " where KPI_ID = ?";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = kpi_id.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			d_v.DBName = (const char*)rs[index++];
			d_v.Value  = (const char*)rs[index++];
			d_v.CNName = (const char*)rs[index++];

			differ.FetchDBDimVal(d_v);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_DIM_VALUE, "[DB2] Select %s failed! (KPI_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabDimValue.c_str(), kpi_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s successfully! (KPI_ID:%s) [DIM_VAL size:%lu]", m_tabDimValue.c_str(), kpi_id.c_str(), differ.GetDBDimValSize());
}

void CAnaDB2::InsertNewDimValue(std::vector<DimVal>& vec_dv) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		std::string sql = "insert into " + m_tabDimValue;
		sql += "(KPI_ID, DB_NAME, DIM_VAL, VAL_CNAME) values(?, ?, ?, ?)";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		Begin();

		const size_t V_SIZE = vec_dv.size();
		for ( size_t i = 0; i < V_SIZE; ++i )
		{
			DimVal& dv = vec_dv[i];

			rs.Parameter(1) = dv.KpiID.c_str();
			rs.Parameter(2) = dv.DBName.c_str();
			rs.Parameter(3) = dv.Value.c_str();
			rs.Parameter(4) = dv.CNName.c_str();

			rs.Execute();

			// 每达到最大值提交一次
			if ( (i % DB_MAX_COMMIT) == 0 && i != 0 )
			{
				//m_pLog->Output("[DB2] [%s] To commit: %lu", m_tabDimValue.c_str(), i);
				Commit();
			}
		}

		//m_pLog->Output("[DB2] [%s] Final commit.", m_tabDimValue.c_str());
		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_INS_DIM_VALUE, "[DB2] Insert new dim_value to %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDimValue.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Insert new dim_value to [%s]: %lu", m_tabDimValue.c_str(), vec_dv.size());
}

void CAnaDB2::InsertResultData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception)
{
	if ( db_info.db2_sql.empty() )
	{
		throw base::Exception(ANAERR_INS_RESULT_DATA, "[DB2] Insert result data failed: NO sql to be executed ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Insert result data to table: [%s]", db_info.target_table.c_str());
	ResultDataInsert(db_info.db2_sql, vec2_data);
	m_pLog->Output("[DB2] Insert result data successfully, size: %llu", vec2_data.size());
}

size_t CAnaDB2::SelectResultData(const std::string& tab_name, const std::string& condition) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	m_pLog->Output("[DB2] Select result data size from: [%s]", tab_name.c_str());

	size_t num_of_data = 0;
	try
	{
		std::string sql = "select count(0) from " + tab_name;
		// 是否带条件
		if ( !condition.empty() )
		{
			sql += " where " + condition;
		}
		m_pLog->Output("[DB2] Execute sql: %s", sql.c_str());

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			num_of_data = (int64_t)rs[1];

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_REPORT_DATA, "[DB2] Select report statistics data failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select the result data size: %llu", num_of_data);
	return num_of_data;
}

void CAnaDB2::DeleteResultData(AnaDBInfo& db_info, bool delete_all) throw(base::Exception)
{
	if ( delete_all )		// 清空全表数据
	{
		m_pLog->Output("[DB2] Empty result table: [%s]", db_info.target_table.c_str());
		AlterEmptyTable(db_info.target_table);

		// 若存在备份表，则将备份表数据一并清除
		if ( !db_info.backup_table.empty() )
		{
			m_pLog->Output("[DB2] Empty backup table: [%s]", db_info.backup_table.c_str());
			AlterEmptyTable(db_info.backup_table);
		}
	}
	else	// 删除指定数据
	{
		// 时间条件
		std::string dt_condition = db_info.GetEtlDayFieldName() + " = '";
		std::string etl_datetime = db_info.GetEtlDay();
		dt_condition += etl_datetime + "'";

		size_t result_data_size = SelectResultData(db_info.target_table, dt_condition);
		if ( result_data_size > 0 )
		{
			m_pLog->Output("[DB2] Delete result data from result table: [%s] (DATE_TIME: %s)", db_info.target_table.c_str(), etl_datetime.c_str());
			DeleteFromTable(db_info.target_table, dt_condition);
		}
		else
		{
			m_pLog->Output("[DB2] NO result data in result table: [%s] (DATE_TIME: %s)", db_info.target_table.c_str(), etl_datetime.c_str());
		}

		// 若存在备份表，则将备份表数据一并清除
		if ( !db_info.backup_table.empty() )
		{
			result_data_size = SelectResultData(db_info.backup_table, dt_condition);
			if ( result_data_size > 0 )
			{
				m_pLog->Output("[DB2] Delete result data from backup table: [%s] (DATE_TIME: %s)", db_info.backup_table.c_str(), etl_datetime.c_str());
				DeleteFromTable(db_info.backup_table, dt_condition);
			}
			else
			{
				m_pLog->Output("[DB2] NO result data in backup table: [%s] (DATE_TIME: %s)", db_info.backup_table.c_str(), etl_datetime.c_str());
			}
		}
	}
}

void CAnaDB2::DeleteTimeResultData(AnaDBInfo& db_info, int beg_time, int end_time) throw(base::Exception)
{
	const std::string ETLDAY_FIELD_NAME = db_info.GetEtlDayFieldName();

	// 时间条件
	std::string dt_condition;
	if ( end_time <= 0 )
	{
		base::PubStr::SetFormatString(dt_condition, "%s = '%d'", ETLDAY_FIELD_NAME.c_str(), beg_time);
	}
	else
	{
		base::PubStr::SetFormatString(dt_condition, "%s >= '%d' and %s <= '%d'", ETLDAY_FIELD_NAME.c_str(), beg_time, ETLDAY_FIELD_NAME.c_str(), end_time);
	}

	size_t result_data_size = SelectResultData(db_info.target_table, dt_condition);
	if ( result_data_size > 0 )
	{
		m_pLog->Output("[DB2] Delete result data from result table: [%s] (BEG_TIME:%d, END_TIME:%d)", db_info.target_table.c_str(), beg_time, end_time);
		DeleteFromTable(db_info.target_table, dt_condition);
	}
	else
	{
		m_pLog->Output("[DB2] NO result data in result table: [%s] (BEG_TIME:%d, END_TIME:%d)", db_info.target_table.c_str(), beg_time, end_time);
	}

	// 若存在备份表，则将备份表数据一并清除
	if ( !db_info.backup_table.empty() )
	{
		result_data_size = SelectResultData(db_info.backup_table, dt_condition);
		if ( result_data_size > 0 )
		{
			m_pLog->Output("[DB2] Delete result data from backup table: [%s] (BEG_TIME:%d, END_TIME:%d)", db_info.backup_table.c_str(), beg_time, end_time);
			DeleteFromTable(db_info.backup_table, dt_condition);
		}
		else
		{
			m_pLog->Output("[DB2] NO result data in backup table: [%s] (BEG_TIME:%d, END_TIME:%d)", db_info.backup_table.c_str(), beg_time, end_time);
		}
	}
}

void CAnaDB2::InsertReportStatData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_reportdata) throw(base::Exception)
{
	if ( db_info.db2_sql.empty() )
	{
		throw base::Exception(ANAERR_INS_RESULT_DATA, "[DB2] Insert report statistics data failed: NO sql to be executed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 入库报表结果表
	m_pLog->Output("[DB2] Insert report statistics data to result table: [%s]", db_info.target_table.c_str());
	ResultDataInsert(db_info.db2_sql, vec2_reportdata);

	// 报表备份表替换报表结果表
	const size_t TARGET_TAB_POS = db_info.db2_sql.find(db_info.target_table);
	std::string ins_backup_sql = db_info.db2_sql;
	ins_backup_sql.replace(TARGET_TAB_POS, db_info.target_table.size(), db_info.backup_table);

	// 入库报表结果备份表
	m_pLog->Output("[DB2] Insert report statistics data to backup table: [%s]", db_info.backup_table.c_str());
	ResultDataInsert(ins_backup_sql, vec2_reportdata);
	m_pLog->Output("[DB2] Insert report statistics data successfully, size: %llu", vec2_reportdata.size());
}

void CAnaDB2::ExecuteSQL(const std::string& exe_sql) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		rs.Prepare(exe_sql.c_str(), XDBO2::CRecordset::forwardOnly);
		m_pLog->Output("[DB2] Execute SQL: %s", exe_sql.c_str());

		rs.Execute();
		Commit();
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_EXECUTE_SQL, "[DB2] Execute SQL failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::SelectTargetData(AnaDBInfo& db_info, const std::string& date, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	// 组织查询 SQL 语句
	std::string sql = "select ";
	const int FIELD_SIZE = db_info.GetFieldSize();
	for ( int i = 0; i < FIELD_SIZE; ++i )
	{
		if ( i != 0 )
		{
			sql += ", " + db_info.GetAnaField(i).field_name;
		}
		else
		{
			sql += db_info.GetAnaField(i).field_name;
		}
	}
	sql += " from " + db_info.target_table;

	// 是否带采集时间
	if ( db_info.IsEtlDayValid() )
	{
		sql += " where " + db_info.GetEtlDayFieldName() + " = ?";
	}
	m_pLog->Output("[DB2] Select targat table [%s], SQL: %s", db_info.target_table.c_str(), sql.c_str());

	std::vector<std::string> v1_dat;
	std::vector<std::vector<std::string> > v2_data;

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		// 是否带时间条件
		if ( db_info.IsEtlDayValid() )
		{
			rs.Parameter(1) = date.c_str();
		}

		rs.Execute();

		while ( !rs.IsEOF() )
		{
			for ( int i = 0; i < FIELD_SIZE; ++i )
			{
				v1_dat.push_back((const char*)rs[i+1]);
			}

			base::PubStr::VVectorSwapPushBack(v2_data, v1_dat);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_ETL_RULE, "[DB2] Select targat table %s failed! [CDBException] %s [FILE:%s, LINE:%d]", db_info.target_table.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select targat table [%s], data size: %llu", db_info.target_table.c_str(), v2_data.size());

	v2_data.swap(vec2_data);
}

void CAnaDB2::SelectEtlRule(OneEtlRule& one) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	int counter = 0;
	try
	{
		std::string sql = "select ETLRULE_TIME, ETLRULE_DATASOURCE, ETLRULE_TARGET, ETLDIM_ID, ETLVAL_ID from " + m_tabEtlRule;
		sql += " where KPI_ID = ? and ETLRULE_ID = ?";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = one.KpiID.c_str();
		rs.Parameter(2) = one.EtlRuleID.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			++counter;

			int index = 1;
			one.EtlTime     = (const char*)rs[index++];
			one.DataSource  = (const char*)rs[index++];
			one.TargetPatch = (const char*)rs[index++];
			one.DimID       = (const char*)rs[index++];
			one.ValID       = (const char*)rs[index++];

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_ETL_RULE, "[DB2] Select %s failed! (KPI_ID:%s, ETLRULE_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ANAERR_SEL_ETL_RULE, "[DB2] Select %s failed! No record (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	// 只取第一组维度
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(one.DimID, "|", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ANAERR_SEL_ETL_RULE, "[DB2] Select %s failed! No ETLDIM_ID! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	else if ( vec_str[0].empty() )
	{
		throw base::Exception(ANAERR_SEL_ETL_RULE, "[DB2] Select %s failed! No ETLDIM_ID! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	else
	{
		one.DimID = vec_str[0];
	}

	// 只取第一组值
	base::PubStr::Str2StrVector(one.ValID, "|", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ANAERR_SEL_ETL_RULE, "[DB2] Select %s failed! No ETLVAL_ID! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	else if ( vec_str[0].empty() )
	{
		throw base::Exception(ANAERR_SEL_ETL_RULE, "[DB2] Select %s failed! No ETLVAL_ID! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	else
	{
		one.ValID = vec_str[0];
	}

	m_pLog->Output("[DB2] Select %s: (KPI_ID:%s, ETLRULE_ID:%s) [ETLRULE_TARGETPATCH:%s] [DIM_ID:%s] [VAL_ID:%s] [Record:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), one.TargetPatch.c_str(), one.DimID.c_str(), one.ValID.c_str(), counter);
}

void CAnaDB2::SelectEtlDim(const std::string& dim_id, std::vector<OneEtlDim>& vec_dim, std::vector<OneEtlDim>& vec_singledim) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<OneEtlDim> v_dim;
	std::vector<OneEtlDim> v_singledim;

	OneEtlDim one;
	one.EtlDimID = dim_id;

	try
	{
		std::string sql = "select ETLDIM_SEQ, ETLDIM_NAME, ETLDIM_MEMO from " + m_tabEtlDim;
		sql += " where ETLDIM_ID = ? order by ETLDIM_SEQ asc";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = dim_id.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			one.EtlDimSeq  = (int)rs[index++];
			one.EtlDimName = (const char*)rs[index++];
			one.SetDimMemoType((const char*)rs[index++]);

			if ( one.EtlDimSeq < 0 )
			{
				m_pLog->Output("[DB2] Select [%s]: Ignore invalid ETLDIM_SEQ [%d] (ETLDIM_ID:%s, ETLDIM_NAME:%s, ETLDIM_MEMO:%s)", 
					m_tabEtlDim.c_str(), one.EtlDimSeq, dim_id.c_str(), one.EtlDimName.c_str(), one.GetDimMemoTypeStr().c_str());
			}
			else
			{
				if ( one.IsShowUp() )	// 单独显示的维度
				{
					v_singledim.push_back(one);
				}
				else	// 一般维度
				{
					v_dim.push_back(one);
				}
			}

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_ETL_DIM, "[DB2] Select %s failed! (ETLDIM_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_dim.empty() )
	{
		throw base::Exception(ANAERR_SEL_ETL_DIM, "[DB2] Select %s failed! No etl_dim record! (ETLDIM_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s successfully! (ETLDIM_ID:%s) [DIM size:%lu] [SINGLE_DIM size:%lu]", m_tabEtlDim.c_str(), dim_id.c_str(), v_dim.size(), v_singledim.size());

	v_dim.swap(vec_dim);
	v_singledim.swap(vec_singledim);
}

void CAnaDB2::SelectEtlVal(const std::string& val_id, std::vector<OneEtlVal>& vec_val) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<OneEtlVal> v_val;
	OneEtlVal one;
	one.EtlValID = val_id;

	try
	{
		std::string sql = "select ETLVAL_SEQ, ETLVAL_NAME, ETLVAL_MEMO from " + m_tabEtlVal;
		sql += " where ETLVAL_ID = ? order by ETLVAL_SEQ asc";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = val_id.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			one.EtlValSeq  = (int)rs[index++];
			one.EtlValName = (const char*)rs[index++];
			one.SetValMemoType((const char*)rs[index++]);

			if ( one.EtlValSeq < 0 )
			{
				m_pLog->Output("[DB2] Select [%s]: Ignore invalid ETLVAL_SEQ [%d] (ETLVAL_ID:%s, ETLVAL_NAME:%s, ETLVAL_MEMO:%s)", 
					m_tabEtlVal.c_str(), one.EtlValSeq, val_id.c_str(), one.EtlValName.c_str(), one.GetValMemoTypeStr().c_str());
			}
			else
			{
				v_val.push_back(one);
			}

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_ETL_VAL, "[DB2] Select %s failed! (ETLVAL_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlVal.c_str(), val_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_val.empty() )
	{
		throw base::Exception(ANAERR_SEL_ETL_VAL, "[DB2] Select %s failed! No record (ETLVAL_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlVal.c_str(), val_id.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s successfully! (ETLVAL_ID:%s) [Record:%lu]", m_tabEtlVal.c_str(), val_id.c_str(), v_val.size());

	v_val.swap(vec_val);
}

void CAnaDB2::SelectAnaRule(AnalyseRule& ana) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string ana_type;
	//std::string cond_type;
	int counter = 0;

	try
	{
		//std::string sql = "select ANALYSIS_NAME, ANALYSIS_TYPE, ANALYSIS_EXPRESSION, ANALYSIS_CONDITION_TYPE, ANALYSIS_CONDITION from ";
		std::string sql = "select ANALYSIS_NAME, ANALYSIS_TYPE, ANALYSIS_EXPRESSION, REMARK_1, REMARK_2, REMARK_3, REMARK_4, REMARK_5 from ";
		sql += (m_tabAnaRule + " where ANALYSIS_ID = '" + ana.AnaID + "'");

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			++counter;

			int index = 1;

			ana.AnaName      = (const char*)rs[index++];
			ana_type         = (const char*)rs[index++];
			ana.AnaExpress   = (const char*)rs[index++];
			//cond_type        = (const char*)rs[index++];
			//ana.AnaCondition = (const char*)rs[index++];
			ana.Remark_1     = (const char*)rs[index++];
			ana.Remark_2     = (const char*)rs[index++];
			ana.Remark_3     = (const char*)rs[index++];
			ana.Remark_4     = (const char*)rs[index++];
			ana.Remark_5     = (const char*)rs[index++];

			if ( !ana.SetAnalyseType(ana_type) )
			{
				throw base::Exception(ANAERR_SEL_ANA_RULE, "[DB2] Select %s failed! (ANALYSIS_ID:%s) 无法识别的分析规则类型: %s [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), ana_type.c_str(), __FILE__, __LINE__);
			}

			//if ( !ana.SetAnalyseConditionType(cond_type) )
			//{
			//	throw base::Exception(ANAERR_SEL_ANA_RULE, "[DB2] Select %s failed! (ANALYSIS_ID:%s) 无法识别的分析条件类型: %s [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), cond_type.c_str(), __FILE__, __LINE__);
			//}

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_ANA_RULE, "[DB2] Select %s failed! (ANALYSIS_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ANAERR_SEL_ANA_RULE, "[DB2] Select %s failed! No record! (ANALYSIS_ID:%s) [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s: [ANALYSIS_ID:%s] [ANALYSIS_NAME:%s] [ANALYSIS_TYPE:%s] [ANALYSIS_EXPRESSION:%s] [Record:%d]", 
		m_tabAnaRule.c_str(), ana.AnaID.c_str(), ana.AnaName.c_str(), ana_type.c_str(), ana.AnaExpress.c_str(), counter);
}

void CAnaDB2::SelectCompareResultDesc(const std::string& kpi_id, const std::string& comp_res_name, std::vector<std::string>& vec_comresdesc)
{
	if ( comp_res_name.empty() )
	{
		m_pLog->Output("<WARNING> [DB2] The DB_Name of compare result is a blank! [KPI_ID:%s]", kpi_id.c_str());
	}

	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		std::string sql = "select DIM_VAL from " + m_tabDimValue + " where KPI_ID = '";
		sql += kpi_id + "' and DB_NAME = '" + comp_res_name + "' order by VAL_CNAME";
		m_pLog->Output("[DB2] Select compare result description, SQL: %s", sql.c_str());

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		std::vector<std::string> vec_desc;
		while ( !rs.IsEOF() )
		{
			vec_desc.push_back((const char*)rs[1]);

			rs.MoveNext();
		}
		rs.Close();

		m_pLog->Output("[DB2] Select compare result description, size: %lu", vec_desc.size());
		vec_desc.swap(vec_comresdesc);
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_SEL_COM_RES_DESC, "[DB2] Select compare result description from %s failed! (KPI_ID:%s, DIM_NAME:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabDimValue.c_str(), kpi_id.c_str(), comp_res_name.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::SelectYCStatRule(const std::string& kpi_id, std::vector<YCStatInfo>& vec_ycsi) throw(base::Exception)
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
			yc_si.statdim_id    = (const char*)rs[index++];
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

std::string CAnaDB2::GetCompareResultName(std::vector<KpiColumn>& vec_kpival)
{
	const int VEC_VAL_SIZE = vec_kpival.size();
	for ( int i = 0; i < VEC_VAL_SIZE; ++i )
	{
		KpiColumn& ref_kpi = vec_kpival[i];

		if ( KpiColumn::EWTYPE_COMPARE_RESULT == ref_kpi.ExpWay )
		{
			return ref_kpi.DBName;
		}
	}

	m_pLog->Output("[DB2] Can not find compare result db_name in KpiColumn val vector!");
	// 找不到，返回空值
	return std::string();
}

void CAnaDB2::AlterEmptyTable(const std::string& tab_name) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		std::string sql = "alter table " + tab_name;
		sql += " activate not logged initially with empty table";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		m_pLog->Output("[DB2] Execute alter sql: %s", sql.c_str());

		rs.Execute();
		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_ALTER_EMPTY_TAB, "[DB2] Alter table [%s] to empty table failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::DeleteFromTable(const std::string& tab_name, const std::string& condition) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		std::string sql = "delete from " + tab_name;
		sql += " where " + condition;

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		m_pLog->Output("[DB2] Execute delete sql: %s", sql.c_str());

		Begin();
		rs.Execute();
		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_DEL_FROM_TAB, "[DB2] Delete result data from [%s] failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::ResultDataInsert(const std::string& db_sql, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception)
{
	if ( vec2_data.empty() )
	{
		m_pLog->Output("[DB2] <WARNING> No result data to be inserted!");
		return;
	}

	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		rs.Prepare(db_sql.c_str(), XDBO2::CRecordset::forwardOnly);
		m_pLog->Output("[DB2] Execute insert result data SQL: %s", db_sql.c_str());

		Begin();

		const size_t V2_DATA_SIZE = vec2_data.size();
		for ( size_t i = 0; i < V2_DATA_SIZE; ++i )
		{
			std::vector<std::string>& ref_vec_data = vec2_data[i];

			const int REF_DATA_SIZE = ref_vec_data.size();
			for ( int j = 0; j < REF_DATA_SIZE; ++j )
			{
				rs.Parameter(j+1) = ref_vec_data[j].c_str();
			}

			rs.Execute();

			// 每达到最大值提交一次
			if ( (i % DB_MAX_COMMIT) == 0 && i != 0 )
			{
				Commit();
			}
		}

		// 最后再提交一次
		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ANAERR_INS_RESULT_DATA, "[DB2] Insert result-data failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::UpdateTaskScheLogState(int log, const std::string& end_time, const std::string& state, const std::string& state_desc, const std::string& remark) throw(base::Exception)
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

void CAnaDB2::UpdateInsertYCDIffSummary(const AnaDBInfo& db_info, const YCStatResult& ycsr) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	const std::string ETL_DAY = db_info.GetEtlDay();
	const std::string NOW_DAY = db_info.GetNowDay();
	std::string sql = "SELECT COUNT(0) FROM " + db_info.target_table + " WHERE STAT_REPORT = ? ";
	sql += "and STAT_ID = ? and STATDIM_ID = ? and STAT_DATE = ? and STAT_CITY = ?";

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = ycsr.stat_report.c_str();
		rs.Parameter(index++) = ycsr.stat_id.c_str();
		rs.Parameter(index++) = ycsr.statdim_id.c_str();
		rs.Parameter(index++) = ETL_DAY.c_str();
		rs.Parameter(index++) = ycsr.stat_city.c_str();
		rs.Execute();

		int num = 0;
		while ( !rs.IsEOF() )
		{
			num = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();

		m_pLog->Output("[DB2] DIFF SUMMARY DIM_ID=[%s], COUNT:%d", ycsr.statdim_id.c_str(), num);
		// 差异汇总维度数据是否存在？
		if ( num > 0 )	// 已存在
		{
			sql  = "UPDATE " + db_info.target_table + " SET STAT_NAME = ?, STAT_VALUE = ?, INTIME = ?, STAT_NUM = ? ";
			sql += "WHERE STAT_REPORT = ? and STAT_ID = ? and STATDIM_ID = ? and STAT_DATE = ? and STAT_CITY = ?";
			m_pLog->Output("[DB2] UPDATE DIFF SUMMARY: REPORT=[%s], STAT_ID=[%s], STAT_NAME=[%s], CITY=[%s], BATCH=[%d], ETL_DAY=[%s], NOW_DAY=[%s], DIM=[%s], VALUE=[%s]", ycsr.stat_report.c_str(), ycsr.stat_id.c_str(), ycsr.stat_name.c_str(), ycsr.stat_city.c_str(), ycsr.stat_batch, ETL_DAY.c_str(), NOW_DAY.c_str(), ycsr.statdim_id.c_str(), ycsr.stat_value.c_str());

			rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

			index = 1;
			rs.Parameter(index++) = ycsr.stat_name.c_str();
			rs.Parameter(index++) = ycsr.stat_value.c_str();
			rs.Parameter(index++) = NOW_DAY.c_str();
			rs.Parameter(index++) = ycsr.stat_batch;
			rs.Parameter(index++) = ycsr.stat_report.c_str();
			rs.Parameter(index++) = ycsr.stat_id.c_str();
			rs.Parameter(index++) = ycsr.statdim_id.c_str();
			rs.Parameter(index++) = ETL_DAY.c_str();
			rs.Parameter(index++) = ycsr.stat_city.c_str();

			rs.Execute();
			Commit();
			rs.Close();
		}
		else	// 不存在
		{
			sql  = "INSERT INTO " + db_info.target_table + "(STAT_REPORT, STAT_ID, STAT_NAME, STATDIM_ID";
			sql += ", STAT_VALUE, STAT_DATE, INTIME, STAT_CITY, STAT_NUM) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
			m_pLog->Output("[DB2] INSERT DIFF SUMMARY: REPORT=[%s], STAT_ID=[%s], STAT_NAME=[%s], CITY=[%s], BATCH=[%d], ETL_DAY=[%s], NOW_DAY=[%s], DIM=[%s], VALUE=[%s]", ycsr.stat_report.c_str(), ycsr.stat_id.c_str(), ycsr.stat_name.c_str(), ycsr.stat_city.c_str(), ycsr.stat_batch, ETL_DAY.c_str(), NOW_DAY.c_str(), ycsr.statdim_id.c_str(), ycsr.stat_value.c_str());

			rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

			index = 1;
			rs.Parameter(index++) = ycsr.stat_report.c_str();
			rs.Parameter(index++) = ycsr.stat_id.c_str();
			rs.Parameter(index++) = ycsr.stat_name.c_str();
			rs.Parameter(index++) = ycsr.statdim_id.c_str();
			rs.Parameter(index++) = ycsr.stat_value.c_str();
			rs.Parameter(index++) = ETL_DAY.c_str();
			rs.Parameter(index++) = NOW_DAY.c_str();
			rs.Parameter(index++) = ycsr.stat_city.c_str();
			rs.Parameter(index++) = ycsr.stat_batch;

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

