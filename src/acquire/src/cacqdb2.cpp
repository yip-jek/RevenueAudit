#include "cacqdb2.h"
#include <boost/algorithm/string.hpp>
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
}

void CAcqDB2::SelectEtlRule(AcqTaskInfo& info) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string data_source;
	std::string dim_id;
	std::string val_id;
	std::string cond_type;

	int counter = 0;
	try
	{
		std::string sql = "select ETLRULE_TIME, ELTRULE_TYPE, ETLRULE_DATASOURCE, ETLRULE_TARGET, ETLDIM_ID, ETLVAL_ID";
		sql += ", ETL_CONDITION_TYPE, ETL_CONDITION from " + m_tabEtlRule + " where ETLRULE_ID = ? and KPI_ID = ?";

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
			info.EtlRuleTarget = (const char*)rs[index++];
			dim_id             = (const char*)rs[index++];
			val_id             = (const char*)rs[index++];
			cond_type          = (const char*)rs[index++];
			info.EtlCondition  = (const char*)rs[index++];

			if ( !info.SetConditionType(cond_type) )
			{
				throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB] Select %s failed! (KPI_ID:%s, ETLRULE_ID:%s) 无法识别的采集条件类型: %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), info.KpiID.c_str(), info.EtlRuleID.c_str(), cond_type.c_str(), __FILE__, __LINE__);
			}

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB] Select %s failed! (KPI_ID:%s, ETLRULE_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), info.KpiID.c_str(), info.EtlRuleID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB] Select %s failed! No record! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB] Select %s successfully! (KPI_ID:%s, ETLRULE_ID:%s) [Record:%d]", m_tabEtlRule.c_str(), info.KpiID.c_str(), info.EtlRuleID.c_str(), counter);

	boost::split(info.vecEtlRuleDataSrc, data_source, boost::is_any_of(","));
	size_t v_size = info.vecEtlRuleDataSrc.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		std::string& ref_data_src = info.vecEtlRuleDataSrc[i];

		boost::trim(ref_data_src);

		if ( ref_data_src.empty() )
		{
			throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB] 采集数据源(ETLRULE_DATASOURCE:%s)配置不正确: 第%lu个数据源为空值! (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", data_source.c_str(), (i+1), info.EtlRuleID.c_str(), __FILE__, __LINE__);
		}
	}

	// 采集维度规则ID
	std::vector<std::string> vec_id;
	base::PubStr::Str2StrVector(dim_id, ",", vec_id);

	std::vector<AcqEtlDim> vec_dim;
	AcqEtlDim dim;
	v_size = vec_id.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		dim.acqEtlDimID = vec_id[i];

		vec_dim.push_back(dim);
	}
	vec_dim.swap(info.vecEtlRuleDim);

	// 采集值规则ID
	base::PubStr::Str2StrVector(val_id, ",", vec_id);

	std::vector<AcqEtlVal> vec_val;
	AcqEtlVal val;
	v_size = vec_id.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		val.acqEtlValID = vec_id[i];

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
			dim.EtlDimMemo    = (const char*)rs[index++];

			v_dim.push_back(dim);

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_DIM, "[DB] Select %s failed! (ETLDIM_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_dim.empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_DIM, "[DB] Select %s failed! No record! (ETLDIM_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB] Select %s successfully! (ETLDIM_ID:%s) [Record:%lu]", m_tabEtlDim.c_str(), dim_id.c_str(), v_dim.size());

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
		std::string sql = "select ETLVAL_SEQ, ETLVAL_NAME, ETLVAL_DESC, ETLVAL_SRCNAME from ";
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

			v_val.push_back(val);

			rs.MoveNext();
		}

	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_VAL, "[DB] Select %s failed! (ETLVAL_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlVal.c_str(), val_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_val.empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_VAL, "[DB] Select %s failed! No record! (ETLVAL_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlVal.c_str(), val_id.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB] Select %s successfully! (ETLVAL_ID:%s) [Record:%lu]", m_tabEtlVal.c_str(), val_id.c_str(), v_val.size());

	v_val.swap(vec_val);
}

