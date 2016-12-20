#include "cacqdb2.h"
#include "log.h"
#include "pubstr.h"

CAcqDB2::CAcqDB2(const std::string& db_name, const std::string& usr, const std::string& pw)
:BaseDB2(db_name, usr, pw)
{
}

CAcqDB2::~CAcqDB2()
{
}

void CAcqDB2::SetTabKpiRule(const std::string& t_kpirule)
{
	m_tabKpiRule = t_kpirule;
}

void CAcqDB2::SetTabEtlRule(const std::string& t_etlrule)
{
	m_tabEtlRule = t_etlrule;
}

void CAcqDB2::SetTabEtlDim(const std::string& t_etldim)
{
	m_tabEtlDim = t_etldim;
}

void CAcqDB2::SetTabEtlVal(const std::string& t_etlval)
{
	m_tabEtlVal = t_etlval;
}

void CAcqDB2::SetTabEtlSrc(const std::string& t_etlsrc)
{
	m_tabEtlSrc = t_etlsrc;
}

void CAcqDB2::SetTabYCStatRule(const std::string& t_statrule)
{
	m_tabYCStatRule = t_statrule;
}

void CAcqDB2::SetTabYCTaskReq(const std::string& t_yc_taskreq)
{
	m_tabYCTaskReq = t_yc_taskreq;
}

void CAcqDB2::SelectYCTaskReqCity(int seq, std::string& city) throw(base::Exception)
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
			throw base::Exception(ADBERR_SEL_YCTASKREQ_CITY, "[DB2] Select city from YC task request table '%s' failed! [SEQ:%d] NO Record! [FILE:%s, LINE:%d]", m_tabYCTaskReq.c_str(), seq, __FILE__, __LINE__);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_YCTASKREQ_CITY, "[DB2] Select city from YC task request table '%s' failed! [SEQ:%d, REC:%d] [CDBException] %s [FILE:%s, LINE:%d]", m_tabYCTaskReq.c_str(), seq, counter, ex.what(), __FILE__, __LINE__);
	}
}

void CAcqDB2::UpdateYCTaskReq(int seq, const std::string& state, const std::string& state_desc, const std::string& task_desc) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "UPDATE " + m_tabYCTaskReq + " SET TASK_STATUS = ?, STATUS_DESC = ?, TASK_DESC = ? WHERE SEQ_ID = ?";
	m_pLog->Output("[DB2] Update task request: STATE=%s, STATE_DESC=%s (SEQ:%d)", state.c_str(), state_desc.c_str(), seq);

	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		int index = 1;
		rs.Parameter(index++) = state.c_str();
		rs.Parameter(index++) = state_desc.c_str();
		rs.Parameter(index++) = task_desc.c_str();
		rs.Parameter(index++) = seq;
		rs.Execute();

		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_UPD_YC_TASK_REQ, "[DB2] Update task request to table '%s' failed! [SEQ:%d] [CDBException] %s [FILE:%s, LINE:%d]", m_tabYCTaskReq.c_str(), seq, ex.what(), __FILE__, __LINE__);
	}
}

void CAcqDB2::SelectEtlTaskInfo(AcqTaskInfo& info) throw(base::Exception)
{
	// 获取采集规则信息
	SelectEtlRule(info);

	// 获取采集维度信息
	size_t vec_size = info.vecEtlRuleDim.size();
	for ( size_t i = 0; i < vec_size; ++i )
	{
		AcqEtlDim& dim = info.vecEtlRuleDim[i];

		SelectEtlDim(dim.acqEtlDimID, dim.vecEtlDim);
	}

	// 获取采集值信息
	vec_size = info.vecEtlRuleVal.size();
	for ( size_t i = 0; i < vec_size; ++i )
	{
		AcqEtlVal& val = info.vecEtlRuleVal[i];

		SelectEtlVal(val.acqEtlValID, val.vecEtlVal);
	}

	SelectEtlSrc(info.EtlRuleID, info.mapEtlSrc);
}

void CAcqDB2::SelectEtlRule(AcqTaskInfo& info) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string data_source;
	std::string dat_src_type;
	std::string dim_id;
	std::string val_id;
	std::string cond_type;

	int counter = 0;
	try
	{
		std::string sql = "select ETLRULE_TIME, ELTRULE_TYPE, ETLRULE_DATASOURCE, DATASOURCE_TYPE, ETLRULE_TARGET, ETLDIM_ID";
		sql += ", ETLVAL_ID, ETL_CONDITION_TYPE, ETL_CONDITION from " + m_tabEtlRule + " where ETLRULE_ID = ? and KPI_ID = ?";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = info.EtlRuleID.c_str();
		rs.Parameter(2) = info.KpiID.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			++counter;

			int index = 1;

			info.EtlRuleTime   = (const char*)rs[index++];
			info.EtlRuleType   = (const char*)rs[index++];
			data_source        = (const char*)rs[index++];
			dat_src_type       = (const char*)rs[index++];
			info.EtlRuleTarget = (const char*)rs[index++];
			dim_id             = (const char*)rs[index++];
			val_id             = (const char*)rs[index++];
			cond_type          = (const char*)rs[index++];
			info.EtlCondition  = (const char*)rs[index++];

			if ( !info.SetDataSourceType(dat_src_type) )
			{
				throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! (KPI_ID:%s, ETLRULE_ID:%s) 无法识别的数据源类型: %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), info.KpiID.c_str(), info.EtlRuleID.c_str(), dat_src_type.c_str(), __FILE__, __LINE__);
			}

			if ( !info.SetConditionType(cond_type) )
			{
				throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! (KPI_ID:%s, ETLRULE_ID:%s) 无法识别的采集条件类型: %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), info.KpiID.c_str(), info.EtlRuleID.c_str(), cond_type.c_str(), __FILE__, __LINE__);
			}

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! (KPI_ID:%s, ETLRULE_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), info.KpiID.c_str(), info.EtlRuleID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! No record! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB2] Select %s successfully! (KPI_ID:%s, ETLRULE_ID:%s) [Record(s):%d]", m_tabEtlRule.c_str(), info.KpiID.c_str(), info.EtlRuleID.c_str(), counter);

	DataSource data_src;
	data_src.isValid = true;

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(data_source, ",", vec_str);
	size_t v_size = vec_str.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		data_src.srcTabName = vec_str[i];
		if ( data_src.srcTabName.empty() )
		{
			throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] 采集数据源(ETLRULE_DATASOURCE:%s)配置不正确: 第%lu个数据源为空值! (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", data_source.c_str(), (i+1), info.EtlRuleID.c_str(), __FILE__, __LINE__);
		}

		info.vecEtlRuleDataSrc.push_back(data_src);
	}

	// 采集维度规则ID
	base::PubStr::Str2StrVector(dim_id, "|", vec_str);

	std::vector<AcqEtlDim> vec_dim;
	AcqEtlDim dim;
	dim.isValid = true;
	v_size = vec_str.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		dim.acqEtlDimID = vec_str[i];

		vec_dim.push_back(dim);
	}
	vec_dim.swap(info.vecEtlRuleDim);

	// 采集值规则ID
	base::PubStr::Str2StrVector(val_id, "|", vec_str);

	std::vector<AcqEtlVal> vec_val;
	AcqEtlVal val;
	val.isValid = true;
	v_size = vec_str.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		val.acqEtlValID = vec_str[i];

		vec_val.push_back(val);
	}
	vec_val.swap(info.vecEtlRuleVal);
}

void CAcqDB2::SelectEtlDim(const std::string& dim_id, std::vector<OneEtlDim>& vec_dim) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<OneEtlDim> v_dim;

	OneEtlDim dim;
	dim.EtlDimID = dim_id;

	try
	{
		std::string sql = "select ETLDIM_SEQ, ETLDIM_NAME, ETLDIM_DESC, ETLDIM_SRCNAME, ETLDIM_MEMO from ";
		sql += m_tabEtlDim + " where ETLDIM_ID = ? order by ETLDIM_SEQ asc";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = dim_id.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			dim.EtlDimSeq     = (int)rs[index++];
			dim.EtlDimName    = (const char*)rs[index++];
			dim.EtlDimDesc    = (const char*)rs[index++];
			dim.EtlDimSrcName = (const char*)rs[index++];
			//dim.EtlDimMemo    = (const char*)rs[index++];
			dim.SetDimMemoType((const char*)rs[index++]);

			v_dim.push_back(dim);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_DIM, "[DB2] Select %s failed! (ETLDIM_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_dim.empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_DIM, "[DB2] Select %s failed! No record! (ETLDIM_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB2] Select %s successfully! (ETLDIM_ID:%s) [Record(s):%lu]", m_tabEtlDim.c_str(), dim_id.c_str(), v_dim.size());

	v_dim.swap(vec_dim);
}

void CAcqDB2::SelectEtlVal(const std::string& val_id, std::vector<OneEtlVal>& vec_val) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<OneEtlVal> v_val;

	OneEtlVal val;
	val.EtlValID = val_id;

	try
	{
		std::string sql = "select ETLVAL_SEQ, ETLVAL_NAME, ETLVAL_DESC, ETLVAL_SRCNAME, ETLVAL_MEMO from ";
		sql += m_tabEtlVal + " where ETLVAL_ID = ? order by ETLVAL_SEQ asc";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = val_id.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			val.EtlValSeq     = (int)rs[index++];
			val.EtlValName    = (const char*)rs[index++];
			val.EtlValDesc    = (const char*)rs[index++];
			val.EtlValSrcName = (const char*)rs[index++];
			//val.EtlValMemo    = (const char*)rs[index++];
			val.SetValMemoType((const char*)rs[index++]);

			v_val.push_back(val);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_VAL, "[DB2] Select %s failed! (ETLVAL_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlVal.c_str(), val_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_val.empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_VAL, "[DB2] Select %s failed! No record! (ETLVAL_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlVal.c_str(), val_id.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB2] Select %s successfully! (ETLVAL_ID:%s) [Record(s):%lu]", m_tabEtlVal.c_str(), val_id.c_str(), v_val.size());

	v_val.swap(vec_val);
}

void CAcqDB2::SelectEtlSrc(const std::string& etlrule_id, std::map<int, EtlSrcInfo>& map_src) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	int seq = 0;
	std::string stype;
	EtlSrcInfo src_info;
	map_src.clear();

	try
	{
		std::string sql = "select ETLSRC_SEQ, CONDITION_TYPE, CONDITION from ";
		sql += m_tabEtlSrc + " where ETLRULE_ID = '" + etlrule_id + "' order by ETLSRC_SEQ";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			seq   = (int)rs[index++];
			stype = (const char*)rs[index++];
			src_info.condition = (const char*)rs[index++];

			if ( !src_info.SetEtlSrcType(stype) )
			{
				throw base::Exception(ADBERR_SEL_ETL_SRC, "[DB2] Select %s failed! (ETLRULE_ID:%s) 无法识别的数据源条件类型：%s [FILE:%s, LINE:%d]", m_tabEtlSrc.c_str(), etlrule_id.c_str(), stype.c_str(), __FILE__, __LINE__);
			}

			if ( seq <= 0 )
			{
				throw base::Exception(ADBERR_SEL_ETL_SRC, "[DB2] Select %s failed! (ETLRULE_ID:%s) 无效的采集源序号：%d [FILE:%s, LINE:%d]", m_tabEtlSrc.c_str(), etlrule_id.c_str(), seq, __FILE__, __LINE__);
			}

			if ( map_src.find(seq) != map_src.end() )
			{
				throw base::Exception(ADBERR_SEL_ETL_SRC, "[DB2] Select %s failed! (ETLRULE_ID:%s) 重复的采集源序号：%d [FILE:%s, LINE:%d]", m_tabEtlSrc.c_str(), etlrule_id.c_str(), seq, __FILE__, __LINE__);
			}
			else
			{
				map_src[seq] = src_info;
			}

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_SRC, "[DB2] Select %s failed! (ETLRULE_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlSrc.c_str(), etlrule_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s successfully! (ETLRULE_ID:%s) [Record(s):%lu]", m_tabEtlSrc.c_str(), etlrule_id.c_str(), map_src.size());
}

void CAcqDB2::FetchEtlData(const std::string& sql, int data_size, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<std::vector<std::string> > v2_data;
	std::vector<std::string> v_dat;

	try
	{
		m_pLog->Output("[DB2] Execute acquire sql: %s", sql.c_str());

		//rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		//rs.Execute();
		rs.Open(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		while ( !rs.IsEOF() )
		{
			for ( int i = 1; i <= data_size; ++i )
			{
				v_dat.push_back((const char*)rs[i]);
			}

			base::PubStr::VVectorSwapPushBack(v2_data, v_dat);

			rs.MoveNext();
		}
		rs.Close();

		m_pLog->Output("[DB2] Execute acquire sql OK.");

		const size_t V2_DAT_SIZE = v2_data.size();
		if ( 0 == V2_DAT_SIZE )
		{
			m_pLog->Output("<WARNING> [DB2] Fetch no data ! (size: 0)");
		}
		else
		{
			m_pLog->Output("[DB2] Fetch acquire data size: %llu", v2_data.size());
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_FETCH_ETL_DATA, "[DB2] Execute acquire sql failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}

	v2_data.swap(vec2_data);
}

bool CAcqDB2::CheckTableExisted(const std::string& tab_name) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		std::string sql = "select count(0) from syscat.tables where tabname='" + tab_name + "'";
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

		if ( count < 0 )	// 没有返回结果
		{
			throw base::Exception(ADBERR_CHECK_SRC_TAB, "[DB2] Check table '%s' whether exist or not failed: NO result! [FILE:%s, LINE:%d]", tab_name.c_str(), __FILE__, __LINE__);
		}
		else
		{
			return (count > 0);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_CHECK_SRC_TAB, "[DB2] Check table '%s' whether exist or not failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAcqDB2::SelectYCStatRule(const std::string& kpi_id, std::vector<YCInfo>& vec_ycinfo) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	YCInfo yc_info;
	std::vector<YCInfo> v_yc_info;

	try
	{
		std::string sql = "select STATDIM_ID, STAT_SQL from " + m_tabYCStatRule;
		sql += " where STAT_ID = '" + kpi_id + "' and STAT_PRIORITY = '00'";
		m_pLog->Output("[DB2] Select table [%s]: %s", m_tabYCStatRule.c_str(), sql.c_str());

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			yc_info.stat_dimid = (const char*)rs[1];
			yc_info.stat_sql   = (const char*)rs[2];
			v_yc_info.push_back(yc_info);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_YC_STATRULE, "[DB2] Select table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabYCStatRule.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_yc_info.swap(vec_ycinfo);
	m_pLog->Output("[DB2] Select YCRA stat_rule successfully! Record(s): %lu", vec_ycinfo.size());
}

