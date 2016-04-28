#include "canadb2.h"
#include <boost/algorithm/string.hpp>
#include "log.h"
#include "pubstr.h"

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

void CAnaDB2::SetTabAnaRule(const std::string& t_anarule)
{
	m_tabAnaRule = t_anarule;
}

void CAnaDB2::SetTabAlarmRule(const std::string& t_alarmrule)
{
	m_tabAlarmRule = t_alarmrule;
}

void CAnaDB2::SelectAnaTaskInfo(AnaTaskInfo& info) throw(base::Exception)
{
	// 获取指标规则数据
	SelectKpiRule(info);

	// 获取指标字段数据
	SelectKpiColumn(info.KpiID, info.vecKpiDimCol, info.vecKpiValCol);

	// 获取采集规则数据
	size_t v_size = info.vecEtlRule.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		OneEtlRule& one = info.vecEtlRule[i];

		SelectEtlRule(one);
	}

	// 获取分析规则数据
	SelectAnaRule(info.AnaRule);

	// 获取告警规则数据
	v_size = info.vecAlarm.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		AlarmRule& alarm = info.vecAlarm[i];

		SelectAlarmRule(alarm);
	}
}

void CAnaDB2::SelectKpiRule(AnaTaskInfo& info) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string str_etlruleid;
	std::string str_alarmid;

	int counter = 0;
	try
	{
		std::string sql = "select DATA_SOURCE, ETLRULE_ID, KPI_CYCLE, ANALYSIS_ID, ALARM_ID, RESULT_TYPE, KPI_TABLENAME from ";
		sql += m_tabKpiRule + " where KPI_ID = ?";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = info.KpiID.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			++counter;

			int index = 1;

			info.DataSrcType   = (const char*)rs[index++];
			str_etlruleid      = (const char*)rs[index++];
			info.KpiCycle      = (const char*)rs[index++];
			info.AnaRule.AnaID = (const char*)rs[index++];
			str_alarmid        = (const char*)rs[index++];
			info.ResultType    = (const char*)rs[index++];
			info.TableName     = (const char*)rs[index++];

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_KPI_RULE, "[DB] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_KPI_RULE, "[DB] Select %s failed! No record! [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB] Select %s successfully! [KPI_ID:%s] [ANALYSIS_ID:%s] [Record:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), counter);

	// 采集规则集
	std::vector<std::string> vec_id;
	base::PubStr::Str2StrVector(str_etlruleid, ",", vec_id);

	std::vector<OneEtlRule>	vec_etl;
	OneEtlRule one;
	one.KpiID = info.KpiID;
	size_t v_size = vec_id.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		one.EtlRuleID = vec_id[i];

		vec_etl.push_back(one);
	}
	vec_etl.swap(info.vecEtlRule);

	// 告警规则集
	base::PubStr::Str2StrVector(str_alarmid, ",", vec_id);

	std::vector<AlarmRule>	vec_alarm;
	AlarmRule alarm;
	v_size = vec_id.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		alarm.AlarmID = vec_id[i];

		vec_alarm.push_back(alarm);
	}
	vec_alarm.swap(info.vecAlarm);
}

void CAnaDB2::SelectKpiColumn(const std::string& kpi_id, std::vector<KpiColumn>& vec_dim, std::vector<KpiColumn>& vec_val) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<KpiColumn> v_dim;
	std::vector<KpiColumn> v_val;

	KpiColumn col;
	col.KpiID = kpi_id;

	try
	{
		std::string sql = "select COLUMN_TYPE, COLUMN_SEQ, DB_NAME, CN_NAME from ";
		sql += m_tabKpiColumn + " where KPI_ID = ? order by COLUMN_TYPE, COLUMN_SEQ asc";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = kpi_id.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			col.ColType = (const char*)rs[index++];
			col.ColSeq  = (int)rs[index++];
			col.DBName  = (const char*)rs[index++];
			col.CNName  = (const char*)rs[index++];

			boost::trim(col.ColType);
			boost::to_upper(col.ColType);

			if ( "DIM" == col.ColType )		// 维度
			{
				v_dim.push_back(col);
			}
			else if ( "VAL" == col.ColType )		// 值
			{
				v_val.push_back(col);
			}
			else
			{
				throw base::Exception(ADBERR_SEL_KPI_COL, "[DB] Select %s failed! 无法识别的指标字段类型: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), col.ColType.c_str(), __FILE__, __LINE__);
			}

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_KPI_COL, "[DB] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_dim.empty() )
	{
		throw base::Exception(ADBERR_SEL_KPI_COL, "[DB] Select %s failed! 没有维度数据! [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), __FILE__, __LINE__);
	}
	if ( v_val.empty() )
	{
		throw base::Exception(ADBERR_SEL_KPI_COL, "[DB] Select %s failed! 没有值数据! [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB] Select %s successfully! [KPI_ID:%s] [ETLDIM size:%lu] [ETLVAL size:%lu]", m_tabKpiColumn.c_str(), kpi_id.c_str(), v_dim.size(), v_val.size());

	v_dim.swap(vec_dim);
	v_val.swap(vec_val);
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
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_DIM_VALUE, "[DB] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDimValue.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB] Select %s successfully! [KPI_ID:%s] [DIM_VAL size:%lu]", m_tabDimValue.c_str(), kpi_id.c_str(), differ.GetDBDimValSize());
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
				//m_pLog->Output("[DB] [%s] To commit: %lu", m_tabDimValue.c_str(), i);
				Commit();
			}
		}

		//m_pLog->Output("[DB] [%s] Final commit.", m_tabDimValue.c_str());
		Commit();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_INS_DIM_VALUE, "[DB] Insert %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDimValue.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB] Insert [%s]: %lu", m_tabDimValue.c_str(), vec_dv.size());
}

void CAnaDB2::SelectEtlRule(OneEtlRule& one) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	int counter = 0;
	try
	{
		std::string sql = "select ETLRULE_TARGETPATCH from " + m_tabEtlRule;
		sql += " where KPI_ID = ? and ETLRULE_ID = ?";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = one.KpiID.c_str();
		rs.Parameter(2) = one.EtlRuleID.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			++counter;

			one.TargetPatch = (const char*)rs[1];

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB] Select %s failed! No record [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB] Select %s: [KPI_ID:%s] [ETLRULE_ID:%s] [ETLRULE_TARGETPATCH:%s] [Record:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), one.TargetPatch.c_str(), counter);
}

void CAnaDB2::SelectAnaRule(AnalyseRule& ana) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	int counter = 0;
	try
	{
		std::string sql = "select ANALYSIS_NAME, ANALYSIS_TYPE, ANALYSIS_EXPRESSION from ";
		sql += m_tabAnaRule + " where ANALYSIS_ID = ?";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = ana.AnaID.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			++counter;

			int index = 1;

			ana.AnaName    = (const char*)rs[index++];
			ana.AnaType    = (const char*)rs[index++];
			ana.AnaExpress = (const char*)rs[index++];

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ANA_RULE, "[DB] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_ANA_RULE, "[DB] Select %s failed! No record! [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB] Select %s: [ANALYSIS_ID:%s] [ANALYSIS_NAME:%s] [ANALYSIS_TYPE:%s] [ANALYSIS_EXPRESSION:%s] [Record:%d]", 
		m_tabAnaRule.c_str(), ana.AnaID.c_str(), ana.AnaName.c_str(), ana.AnaType.c_str(), ana.AnaExpress.c_str(), counter);
}

void CAnaDB2::SelectAlarmRule(AlarmRule& alarm) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	int counter = 0;
	try
	{
		std::string sql = "select ALARM_NAME, ALARM_TYPE, ALARM_EXPRESSION, ALARM_EVENT, SEND_AMS, SEND_SMS from ";
		sql += m_tabAlarmRule + " where ALARM_ID = ?";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = alarm.AlarmID.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			++counter;

			int index = 1;

			alarm.AlarmName	   = (const char*)rs[index++];
			alarm.AlarmType	   = (const char*)rs[index++];
			alarm.AlarmExpress = (const char*)rs[index++];
			alarm.AlarmEvent   = (const char*)rs[index++];
			alarm.SendAms      = (const char*)rs[index++];
			alarm.SendSms      = (const char*)rs[index++];

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ALARM_RULE, "[DB] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmRule.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_ALARM_RULE, "[DB] Select %s failed! No record! [FILE:%s, LINE:%d]", m_tabAlarmRule.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB] Select %s: [ALARM_ID:%s] [ALARM_NAME:%s] [ALARM_TYPE:%s] [Record:%d]", 
		m_tabAlarmRule.c_str(), alarm.AlarmID.c_str(), alarm.AlarmName.c_str(), alarm.AlarmType.c_str(), counter);
}

