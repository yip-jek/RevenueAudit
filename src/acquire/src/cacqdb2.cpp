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

CAcqDB2::SetTabKpiRule(const std::string& t_kpirule)
{
	m_tabKpiRule = t_kpirule;
}

CAcqDB2::SetTabEtlRule(const std::string& t_etlrule)
{
	m_tabEtlRule = t_etlrule;
}

CAcqDB2::SetTabEtlDim(const std::string& t_etldim)
{
	m_tabEtlDim = t_etldim;
}

CAcqDB2::SetTabEtlVal(const std::string& t_etlval)
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

	try
	{
		std::string sql = "select ETLRULE_TIME, ELTRULE_TYPE, ETLRULE_DATASOURCE, ETLRULE_TARGET, ETLDIM_ID, ETLVAL_ID from ";
		sql += m_tabEtlRule + " where ETLRULE_ID = ? and KPI_ID = ?";

		rs.Prepare(sql.c_str(), CRecordset::forwardOnly);
		rs.Parameter(0) = info.EtlRuleID;
		rs.Parameter(1) = info.KpiID;
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int col = 0;

			info.EtlRuleTime   = (const char*)rs[col++];
			info.EtlRuleType   = (const char*)rs[col++];
			data_source        = (const char*)rs[col++];
			info.EtlRuleTarget = (const char*)rs[col++];
			dim_id             = (const char*)rs[col++];
			val_id             = (const char*)rs[col++];
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "Select ETL_RULE failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}

	boost::split(info.vecEtlRuleDataSrc, data_source, boost::is_any_of(","));
	size_t v_size = info.vecEtlRuleDataSrc.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		std::string& ref_data_src = info.vecEtlRuleDataSrc[i];

		boost::trim(ref_data_src);

		if ( ref_data_src.empty() )
		{
			throw base::Exception(ADBERR_SEL_ETL_RULE, "采集数据源(ETLRULE_DATASOURCE:%s)配置不正确: 第%lu个数据源为空值! [FILE:%s, LINE:%d]", data_source.c_str(), (i+1), __FILE__, __LINE__);
		}
	}

	// 采集维度规则ID
	std::vector<int> vec_id;
	base::PubStr::Str2IntVector(dim_id, ",", vec_id);

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
	base::PubStr::Str2IntVector(val_id, ",", vec_id);

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

void CAcqDB2::SelectEtlDim(int dim_id, std::vector<OneEtlDim>& vec_dim) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_DIM, "Select ETL_DIM failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

void CAcqDB2::SelectEtlVal(int val_id, std::vector<OneEtlVal>& vec_val) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_VAL, "Select ETL_VAL failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

