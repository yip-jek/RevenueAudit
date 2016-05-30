#include "canadb2.h"
#include "log.h"
#include "pubstr.h"
#include "simpletime.h"
#include "anadbinfo.h"
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

void CAnaDB2::SetTabAlarmRule(const std::string& t_alarmrule)
{
	m_tabAlarmRule = t_alarmrule;
}

void CAnaDB2::SetTabAlarmEvent(const std::string& t_alarmevent)
{
	m_tabAlarmEvent = t_alarmevent;
}

void CAnaDB2::SetTabDictChannel(const std::string& t_dictchann)
{
	m_tabDictChannel = t_dictchann;
}

void CAnaDB2::SetTabDictCity(const std::string& t_dictcity)
{
	m_tabDictCity = t_dictcity;
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

		// 获取采集维度信息
		SelectEtlDim(one.DimID, one.vecEtlDim);

		// 获取采集值信息
		SelectEtlVal(one.ValID, one.vecEtlVal);
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
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_CHANN_UNICODE, "[DB2] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDictChannel.c_str(), ex.what(), __FILE__, __LINE__);
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
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_CITY_UNICODE, "[DB2] Select %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDictCity.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s successfully! [Record: %llu]", m_tabDictCity.c_str(), v_city_uc.size());

	v_city_uc.swap(vec_cityunicode);
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

		std::string result_type;

		while ( !rs.IsEOF() )
		{
			++counter;

			int index = 1;

			info.DataSrcType   = (const char*)rs[index++];
			str_etlruleid      = (const char*)rs[index++];
			info.KpiCycle      = (const char*)rs[index++];
			info.AnaRule.AnaID = (const char*)rs[index++];
			str_alarmid        = (const char*)rs[index++];
			result_type 	   = (const char*)rs[index++];
			info.TableName     = (const char*)rs[index++];

			if ( !info.SetTableType(result_type) )
			{
				throw base::Exception(ADBERR_SEL_KPI_RULE, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的结果表类型: %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), result_type.c_str(), __FILE__, __LINE__);
			}

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_KPI_RULE, "[DB2] Select %s failed! (KPI_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_KPI_RULE, "[DB2] Select %s failed! No record! (KPI_ID:%s) [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB2] Select %s successfully! (KPI_ID:%s, ANALYSIS_ID:%s) [Record:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), counter);

	// 采集规则集
	std::vector<std::string> vec_id;
	base::PubStr::Str2StrVector(str_etlruleid, "|", vec_id);

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
	base::PubStr::Str2StrVector(str_alarmid, "|", vec_id);

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
		std::string sql = "select COLUMN_TYPE, COLUMN_SEQ, DB_NAME, CN_NAME, DIS_TYPE from ";
		sql += m_tabKpiColumn + " where KPI_ID = ? order by COLUMN_TYPE, COLUMN_SEQ asc";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = kpi_id.c_str();
		rs.Execute();

		std::string col_type;
		std::string dis_type;

		while ( !rs.IsEOF() )
		{
			int index = 1;

			col_type    = (const char*)rs[index++];
			col.ColSeq  = (int)rs[index++];
			col.DBName  = (const char*)rs[index++];
			col.CNName  = (const char*)rs[index++];
			dis_type  = (const char*)rs[index++];

			if ( !col.SetDisplayType(dis_type) )
			{
				throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的前台显示方式: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), kpi_id.c_str(), dis_type.c_str(), __FILE__, __LINE__);
			}

			if ( col.SetColumnType(col_type) )
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
					throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的指标字段类型: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), kpi_id.c_str(), col_type.c_str(), __FILE__, __LINE__);
				}
			}
			else
			{
				throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的指标字段类型: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), kpi_id.c_str(), col_type.c_str(), __FILE__, __LINE__);
			}

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), kpi_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_dim.empty() )
	{
		throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 没有维度数据! [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), kpi_id.c_str(), __FILE__, __LINE__);
	}
	if ( v_val.empty() )
	{
		throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 没有值数据! [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), kpi_id.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB2] Select %s successfully! (KPI_ID:%s) [ETLDIM size:%lu] [ETLVAL size:%lu]", m_tabKpiColumn.c_str(), kpi_id.c_str(), v_dim.size(), v_val.size());

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
		throw base::Exception(ADBERR_SEL_DIM_VALUE, "[DB2] Select %s failed! (KPI_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabDimValue.c_str(), kpi_id.c_str(), ex.what(), __FILE__, __LINE__);
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
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_INS_DIM_VALUE, "[DB2] Insert %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDimValue.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Insert [%s]: %lu", m_tabDimValue.c_str(), vec_dv.size());
}

void CAnaDB2::InsertResultData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_fields) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	if ( db_info.db2_sql.empty() )
	{
		throw base::Exception(ADBERR_INS_RESULT_DATA, "[DB2] Insert result data to [%s] failed: no sql to be executed! [FILE:%s, LINE:%d]", db_info.target_table.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB2] Insert result data to [%s], SQL: %s", db_info.target_table.c_str(), db_info.db2_sql.c_str());

	const size_t FIELDS_SIZE = vec2_fields.size();
	try
	{
		rs.Prepare(db_info.db2_sql.c_str(), XDBO2::CRecordset::forwardOnly);

		Begin();

		if ( db_info.time_stamp )		// 带时间戳
		{
			//// 时间格式：YYYYMMDDHHMISS (24小时制)
			//// 例如：20160501121212 表示2016年05月01日12时12分12秒
			//std::string date_time = base::SimpleTime::Now().Time14();

			for ( size_t i = 0; i < FIELDS_SIZE; ++i )
			{
				std::vector<std::string>& v_data = vec2_fields[i];

				const int DATA_SIZE = v_data.size();
				for ( int j = 0; j < DATA_SIZE; ++j )
				{
					rs.Parameter(j+1) = v_data[j].c_str();
				}

				// 绑定时间戳
				//rs.Parameter(DATA_SIZE+1) = date_time.c_str();
				rs.Parameter(DATA_SIZE+1) = db_info.date_time.c_str();

				rs.Execute();

				// 每达到最大值提交一次
				if ( (i % DB_MAX_COMMIT) == 0 && i != 0 )
				{
					Commit();
				}
			}
		}
		else		// 不带时间戳
		{
			for ( size_t i = 0; i < FIELDS_SIZE; ++i )
			{
				std::vector<std::string>& v_data = vec2_fields[i];

				const int DATA_SIZE = v_data.size();
				for ( int j = 0; j < DATA_SIZE; ++j )
				{
					rs.Parameter(j+1) = v_data[j].c_str();
				}

				rs.Execute();

				// 每达到最大值提交一次
				if ( (i % DB_MAX_COMMIT) == 0 && i != 0 )
				{
					Commit();
				}
			}
		}

		// 最后提交一次
		Commit();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_INS_RESULT_DATA, "[DB2] Insert result data to [%s] failed! [CDBException] %s [FILE:%s, LINE:%d]", db_info.target_table.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Insert result data to [%s]: %llu", db_info.target_table.c_str(), FIELDS_SIZE);
}

size_t CAnaDB2::SelectReportStatData(AnaDBInfo& db_info, const std::string& day_time) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	m_pLog->Output("[DB2] Select [%s] report statistics data ...", db_info.target_table.c_str());

	size_t num_of_data = 0;

	try
	{
		std::string sql = "select count(0) from " + db_info.target_table;
		// 时间字段在末尾
		sql += " where " + db_info.vec_fields[db_info.vec_fields.size()-1] + " = ?";

		m_pLog->Output("[DB2] Execute sql: %s", sql.c_str());

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = day_time.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			num_of_data = (int64_t)rs[1];

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_REPORT_DATA, "[DB2] Select report statistics data failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}

	return num_of_data;
}

void CAnaDB2::DeleteReportStatData(AnaDBInfo& db_info, const std::string& day_time) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	m_pLog->Output("[DB2] Delete report statistics data from [%s], date_time:%s", db_info.target_table.c_str(), day_time.c_str());

	try
	{
		std::string sql = "delete from " + db_info.target_table;
		// 时间字段在末尾
		sql += " where " + db_info.vec_fields[db_info.vec_fields.size()-1] + " = ?";

		m_pLog->Output("[DB2] Execute sql: %s", sql.c_str());

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		Begin();

		rs.Parameter(1) = day_time.c_str();
		rs.Execute();

		Commit();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_DEL_REPORT_DATA, "[DB2] Delete report statistics data from [%s] failed! [CDBException] %s [FILE:%s, LINE:%d]", db_info.target_table.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::InsertReportStatData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_reportdata) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	if ( db_info.db2_sql.empty() )
	{
		throw base::Exception(ADBERR_INS_REPORT_DATA, "[DB2] Insert report statistics data to [%s] failed: no sql to be executed! [FILE:%s, LINE:%d]", db_info.target_table.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB2] Insert report statistics data to [%s], SQL: %s", db_info.target_table.c_str(), db_info.db2_sql.c_str());

	const size_t REPORT_DATA_SIZE = vec2_reportdata.size();
	try
	{
		rs.Prepare(db_info.db2_sql.c_str(), XDBO2::CRecordset::forwardOnly);

		Begin();

		//// 时间格式：YYYYMMDD (24小时制)
		//// 例如：20160501 表示2016年05月01日
		//std::string now_day = base::SimpleTime::Now().DayTime8();

		for ( size_t i = 0; i < REPORT_DATA_SIZE; ++i )
		{
			std::vector<std::string>& v_data = vec2_reportdata[i];

			const int DATA_SIZE = v_data.size();
			for ( int j = 0; j < DATA_SIZE; ++j )
			{
				rs.Parameter(j+1) = v_data[j].c_str();
			}

			// 绑定时间戳
			//rs.Parameter(DATA_SIZE+1) = now_day.c_str();
			rs.Parameter(DATA_SIZE+1) = db_info.date_time.c_str();

			rs.Execute();

			// 每达到最大值提交一次
			if ( (i % DB_MAX_COMMIT) == 0 && i != 0 )
			{
				Commit();
			}
		}

		// 最后提交一次
		Commit();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_INS_REPORT_DATA, "[DB2] Insert report statistics data to [%s] failed! [CDBException] %s [FILE:%s, LINE:%d]", db_info.target_table.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Insert report statistics data to [%s]: %llu", db_info.target_table.c_str(), REPORT_DATA_SIZE);
}

void CAnaDB2::SelectEtlRule(OneEtlRule& one) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	int counter = 0;
	try
	{
		std::string sql = "select ETLRULE_TIME, ETLRULE_TARGET, ETLDIM_ID, ETLVAL_ID from " + m_tabEtlRule;
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
			one.TargetPatch = (const char*)rs[index++];
			one.DimID       = (const char*)rs[index++];
			one.ValID       = (const char*)rs[index++];

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! (KPI_ID:%s, ETLRULE_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! No record (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	// 只取第一组维度
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(one.DimID, "|", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! No ETLDIM_ID! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	else if ( vec_str[0].empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! No ETLDIM_ID! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	else
	{
		one.DimID = vec_str[0];
	}

	// 只取第一组值
	base::PubStr::Str2StrVector(one.ValID, "|", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! No ETLVAL_ID! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	else if ( vec_str[0].empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select %s failed! No ETLVAL_ID! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
	else
	{
		one.ValID = vec_str[0];
	}

	m_pLog->Output("[DB2] Select %s: (KPI_ID:%s, ETLRULE_ID:%s) [ETLRULE_TARGETPATCH:%s] [DIM_ID:%s] [VAL_ID:%s] [Record:%d]", m_tabEtlRule.c_str(), one.KpiID.c_str(), one.EtlRuleID.c_str(), one.TargetPatch.c_str(), one.DimID.c_str(), one.ValID.c_str(), counter);
}

void CAnaDB2::SelectEtlDim(const std::string& dim_id, std::vector<OneEtlDim>& vec_dim) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::vector<OneEtlDim> v_dim;
	OneEtlDim one;
	one.EtlDimID = dim_id;

	try
	{
		std::string sql = "select ETLDIM_SEQ, ETLDIM_NAME from " + m_tabEtlDim;
		sql += " where ETLDIM_ID = ? order by ETLDIM_SEQ asc";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = dim_id.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			one.EtlDimSeq  = (int)rs[index++];
			one.EtlDimName = (const char*)rs[index++];

			if ( one.EtlDimSeq < 0 )
			{
				m_pLog->Output("[DB2] Select [%s]: Ignore invalid ETLDIM_SEQ [%d] (ETLDIM_ID:%s, ETLDIM_NAME:%s)", 
					m_tabEtlDim.c_str(), one.EtlDimSeq, dim_id.c_str(), one.EtlDimName.c_str());
			}
			else
			{
				v_dim.push_back(one);
			}

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_DIM, "[DB2] Select %s failed! (ETLDIM_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_dim.empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_DIM, "[DB2] Select %s failed! No record (ETLDIM_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s successfully! (ETLDIM_ID:%s) [Record:%lu]", m_tabEtlDim.c_str(), dim_id.c_str(), v_dim.size());

	v_dim.swap(vec_dim);
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
		std::string sql = "select ETLVAL_SEQ, ETLVAL_NAME from " + m_tabEtlVal;
		sql += " where ETLVAL_ID = ? order by ETLVAL_SEQ asc";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = val_id.c_str();
		rs.Execute();

		while ( !rs.IsEOF() )
		{
			int index = 1;

			one.EtlValSeq  = (int)rs[index++];
			one.EtlValName = (const char*)rs[index++];

			if ( one.EtlValSeq < 0 )
			{
				m_pLog->Output("[DB2] Select [%s]: Ignore invalid ETLVAL_SEQ [%d] (ETLVAL_ID:%s, ETLVAL_NAME:%s)", 
					m_tabEtlVal.c_str(), one.EtlValSeq, val_id.c_str(), one.EtlValName.c_str());
			}
			else
			{
				v_val.push_back(one);
			}

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ETL_VAL, "[DB2] Select %s failed! (ETLVAL_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlVal.c_str(), val_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_val.empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_VAL, "[DB2] Select %s failed! No record (ETLVAL_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlVal.c_str(), val_id.c_str(), __FILE__, __LINE__);
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
		std::string sql = "select ANALYSIS_NAME, ANALYSIS_TYPE, ANALYSIS_EXPRESSION from ";
		sql += m_tabAnaRule + " where ANALYSIS_ID = ?";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Parameter(1) = ana.AnaID.c_str();
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

			if ( !ana.SetAnalyseType(ana_type) )
			{
				throw base::Exception(ADBERR_SEL_ANA_RULE, "[DB2] Select %s failed! (ANALYSIS_ID:%s) 无法识别的分析规则类型: %s [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), ana_type.c_str(), __FILE__, __LINE__);
			}

			//if ( !ana.SetAnalyseConditionType(cond_type) )
			//{
			//	throw base::Exception(ADBERR_SEL_ANA_RULE, "[DB2] Select %s failed! (ANALYSIS_ID:%s) 无法识别的分析条件类型: %s [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), cond_type.c_str(), __FILE__, __LINE__);
			//}

			rs.MoveNext();
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_ANA_RULE, "[DB2] Select %s failed! (ANALYSIS_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_ANA_RULE, "[DB2] Select %s failed! No record! (ANALYSIS_ID:%s) [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s: [ANALYSIS_ID:%s] [ANALYSIS_NAME:%s] [ANALYSIS_TYPE:%s] [ANALYSIS_EXPRESSION:%s] [Record:%d]", 
		m_tabAnaRule.c_str(), ana.AnaID.c_str(), ana.AnaName.c_str(), ana_type.c_str(), ana.AnaExpress.c_str(), counter);
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
		throw base::Exception(ADBERR_SEL_ALARM_RULE, "[DB2] Select %s failed! (ALARM_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmRule.c_str(), alarm.AlarmID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_ALARM_RULE, "[DB2] Select %s failed! No record! (ALARM_ID:%s) [FILE:%s, LINE:%d]", m_tabAlarmRule.c_str(), alarm.AlarmID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select %s: [ALARM_ID:%s] [ALARM_NAME:%s] [ALARM_TYPE:%s] [Record:%d]", 
		m_tabAlarmRule.c_str(), alarm.AlarmID.c_str(), alarm.AlarmName.c_str(), alarm.AlarmType.c_str(), counter);
}

