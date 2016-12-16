#include "canadb2.h"
#include "log.h"
#include "pubstr.h"
#include "anadbinfo.h"
#include "uniformcodetransfer.h"
#include "alarmevent.h"


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

void CAnaDB2::SetTabYCStatRule(const std::string& t_statrule)
{
	m_tabYCStatRule = t_statrule;
}

void CAnaDB2::SetTabYCTaskReq(const std::string& t_yc_taskreq)
{
	m_tabYCTaskReq = t_yc_taskreq;
}

void CAnaDB2::UpdateYCTaskReq(int seq, const std::string& state, const std::string& state_desc, const std::string& task_desc) throw(base::Exception)
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

void CAnaDB2::SelectSequence(const std::string& seq_name, size_t size, std::vector<std::string>& vec_seq) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string sql = "select " + seq_name + ".NEXTVAL from sysibm.sysdummy1";
	m_pLog->Output("[DB2] Select sequence: %s (size: %llu)", sql.c_str(), size);

	std::vector<std::string> v_seq;
	try
	{
		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		while ( size-- > 0 )
		{
			rs.Execute();

			while ( !rs.IsEOF() )
			{
				v_seq.push_back((const char*)rs[1]);

				rs.MoveNext();
			}
		}

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		m_pLog->Output("[DB2] Select sequence '%s' size: %llu", seq_name.c_str(), v_seq.size());
		throw base::Exception(ADBERR_SEL_SEQUENCE, "[DB2] Select sequence '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", seq_name.c_str(), ex.what(), __FILE__, __LINE__);
	}

	v_seq.swap(vec_seq);

	m_pLog->Output("[DB2] Select sequence '%s' size: %llu", seq_name.c_str(), vec_seq.size());
}

void CAnaDB2::SelectAnaTaskInfo(AnaTaskInfo& info) throw(base::Exception)
{
	// 获取指标规则数据
	SelectKpiRule(info);

	// 获取指标字段数据
	SelectKpiColumn(info);

	// 获取采集规则数据
	size_t v_size = info.vecEtlRule.size();
	for ( size_t i = 0; i < v_size; ++i )
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

	// 获取告警规则数据
	v_size = info.vecAlarm.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		AlarmRule& alarm = info.vecAlarm[i];

		SelectAlarmRule(alarm);
	}

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
		rs.Close();
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
		std::string sql = "select DATA_SOURCE, ETLRULE_ID, KPI_CYCLE, ALARM_ID, RESULT_TYPE, KPI_TABLENAME from ";
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
			str_alarmid        = (const char*)rs[index++];
			result_type        = (const char*)rs[index++];
			info.TableName     = (const char*)rs[index++];

			if ( !info.SetTableType(result_type) )
			{
				throw base::Exception(ADBERR_SEL_KPI_RULE, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的结果表类型: %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), result_type.c_str(), __FILE__, __LINE__);
			}

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_KPI_RULE, "[DB2] Select %s failed! (KPI_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( 0 == counter )
	{
		throw base::Exception(ADBERR_SEL_KPI_RULE, "[DB2] Select %s failed! No record! (KPI_ID:%s) [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[DB2] Select %s successfully! (KPI_ID:%s) [Record:%d]", m_tabKpiRule.c_str(), info.KpiID.c_str(), counter);

	// 采集规则集
	std::vector<std::string> vec_id;
	base::PubStr::Str2StrVector(str_etlruleid, "|", vec_id);

	std::vector<OneEtlRule> vec_etl;
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

	std::vector<AlarmRule> vec_alarm;
	AlarmRule alarm;
	v_size = vec_id.size();
	for ( size_t i = 0; i < v_size; ++i )
	{
		alarm.AlarmID = vec_id[i];

		vec_alarm.push_back(alarm);
	}
	vec_alarm.swap(info.vecAlarm);
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
				throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的前台显示方式: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), dis_type.c_str(), __FILE__, __LINE__);
			}

			if ( !col.SetColumnType(col_type) )
			{
				throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的指标字段类型: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), col_type.c_str(), __FILE__, __LINE__);
			}

			if ( !col.SetExpWayType(exp_way) )
			{
				throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的后台表示方式: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), exp_way.c_str(), __FILE__, __LINE__);
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
					throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 无法识别的指标字段类型: %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), col_type.c_str(), __FILE__, __LINE__);
				}
			}

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_dim.empty() )
	{
		throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 没有维度数据! [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), __FILE__, __LINE__);
	}
	if ( v_val.empty() )
	{
		throw base::Exception(ADBERR_SEL_KPI_COL, "[DB2] Select %s failed! (KPI_ID:%s) 没有值数据! [FILE:%s, LINE:%d]", m_tabKpiColumn.c_str(), info.KpiID.c_str(), __FILE__, __LINE__);
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

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_INS_DIM_VALUE, "[DB2] Insert new dim_value to %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabDimValue.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Insert new dim_value to [%s]: %lu", m_tabDimValue.c_str(), vec_dv.size());
}

void CAnaDB2::InsertResultData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception)
{
	if ( db_info.db2_sql.empty() )
	{
		throw base::Exception(ADBERR_INS_RESULT_DATA, "[DB2] Insert result data failed: NO sql to be executed ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
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
		throw base::Exception(ADBERR_SEL_REPORT_DATA, "[DB2] Select report statistics data failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
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
	// 时间条件
	std::string dt_condition;
	if ( end_time <= 0 )
	{
		base::PubStr::SetFormatString(dt_condition, "%s = '%d'", db_info.GetEtlDayFieldName().c_str(), beg_time);
	}
	else
	{
		base::PubStr::SetFormatString(dt_condition, "%s >= '%d' and %s <= '%d'", db_info.GetEtlDayFieldName().c_str(), beg_time, end_time);
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
			m_pLog->Output("[DB2] Delete result data from backup table: [%s] (BEG_TIME:%d, END_TIME:%d)", db_info.target_table.c_str(), beg_time, end_time);
			DeleteFromTable(db_info.backup_table, dt_condition);
		}
		else
		{
			m_pLog->Output("[DB2] NO result data in backup table: [%s] (BEG_TIME:%d, END_TIME:%d)", db_info.target_table.c_str(), beg_time, end_time);
		}
	}
}

void CAnaDB2::InsertReportStatData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_reportdata) throw(base::Exception)
{
	if ( db_info.db2_sql.empty() )
	{
		throw base::Exception(ADBERR_INS_RESULT_DATA, "[DB2] Insert report statistics data failed: NO sql to be executed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
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
		throw base::Exception(ADBERR_SEL_ETL_RULE, "[DB2] Select targat table %s failed! [CDBException] %s [FILE:%s, LINE:%d]", db_info.target_table.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Select targat table [%s], data size: %llu", db_info.target_table.c_str(), v2_data.size());

	v2_data.swap(vec2_data);
}

bool CAnaDB2::SelectMaxAlarmEventID(int& max_event_id) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		std::string sql = "select count(0) from " + m_tabAlarmEvent;

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
		rs.Execute();

		int rec_count = -1;
		while ( !rs.IsEOF() )
		{
			rec_count = (int)rs[1];

			rs.MoveNext();
		}
		rs.Close();

		if ( rec_count > 0 )			// 有记录
		{
			sql = "select max(ALARMEVENT_ID) from " + m_tabAlarmEvent;

			rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);
			rs.Execute();

			while ( !rs.IsEOF() )
			{
				max_event_id = (int)rs[1];

				rs.MoveNext();
			}
			rs.Close();

			return true;
		}
		else if ( 0 == rec_count )		// 无记录
		{
			return false;
		}
		else	// 无返回值
		{
			throw base::Exception(ADBERR_SEL_MAX_EVENTID, "[DB2] %s failed: NO result ! [FILE:%s, LINE:%d]", sql.c_str(), __FILE__, __LINE__);
		}
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_MAX_EVENTID, "[DB2] Select %s max event ID failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmEvent.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::InsertAlarmEvent(std::vector<AlarmEvent>& vec_event) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		std::string sql = "insert into " + m_tabAlarmEvent;
		sql += "(ALARMEVENT_ID, ALARMEVENT_CONT, ALARMEVENT_DESC, ALARM_LEVEL, ALARM_ID, ALARM_TIME, ALARM_STATE) ";
		sql += "values(?, ?, ?, ?, ?, ?, ?)";

		rs.Prepare(sql.c_str(), XDBO2::CRecordset::forwardOnly);

		Begin();

		const int VEC_SIZE = vec_event.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			AlarmEvent& ref_event = vec_event[i];

			rs.Parameter(1) = ref_event.eventID;
			rs.Parameter(2) = ref_event.eventCont.c_str();
			rs.Parameter(3) = ref_event.eventDesc.c_str();
			rs.Parameter(4) = AlarmEvent::TransAlarmLevel(ref_event.alarmLevel).c_str();
			rs.Parameter(5) = ref_event.alarmID.c_str();
			rs.Parameter(6) = ref_event.alarmTime.c_str();
			rs.Parameter(7) = AlarmEvent::TransAlarmState(ref_event.alarmState);

			rs.Execute();

			// 每达到最大值提交一次
			if ( (i % DB_MAX_COMMIT) == 0 && i != 0 )
			{
				Commit();
			}
		}

		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_INS_ALARMEVENT, "[DB2] Insert alarm event to %s failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabAlarmEvent.c_str(), ex.what(), __FILE__, __LINE__);
	}

	m_pLog->Output("[DB2] Insert alarm event to [%s]: %lu", m_tabAlarmEvent.c_str(), vec_event.size());
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
		rs.Close();
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
		throw base::Exception(ADBERR_SEL_ETL_DIM, "[DB2] Select %s failed! (ETLDIM_ID:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), ex.what(), __FILE__, __LINE__);
	}

	if ( v_dim.empty() )
	{
		throw base::Exception(ADBERR_SEL_ETL_DIM, "[DB2] Select %s failed! No etl_dim record! (ETLDIM_ID:%s) [FILE:%s, LINE:%d]", m_tabEtlDim.c_str(), dim_id.c_str(), __FILE__, __LINE__);
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
				throw base::Exception(ADBERR_SEL_ANA_RULE, "[DB2] Select %s failed! (ANALYSIS_ID:%s) 无法识别的分析规则类型: %s [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), ana_type.c_str(), __FILE__, __LINE__);
			}

			//if ( !ana.SetAnalyseConditionType(cond_type) )
			//{
			//	throw base::Exception(ADBERR_SEL_ANA_RULE, "[DB2] Select %s failed! (ANALYSIS_ID:%s) 无法识别的分析条件类型: %s [FILE:%s, LINE:%d]", m_tabAnaRule.c_str(), ana.AnaID.c_str(), cond_type.c_str(), __FILE__, __LINE__);
			//}

			rs.MoveNext();
		}
		rs.Close();
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

	std::string alarm_type;
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

			alarm.AlarmName    = (const char*)rs[index++];
			alarm_type         = (const char*)rs[index++];
			alarm.AlarmExpress = (const char*)rs[index++];
			alarm.AlarmEvent   = (const char*)rs[index++];
			alarm.SendAms      = (const char*)rs[index++];
			alarm.SendSms      = (const char*)rs[index++];

			if ( !alarm.SetAlarmType(alarm_type) )
			{
				throw base::Exception(ADBERR_SEL_ALARM_RULE, "[DB2] Select %s failed! (ALARM_ID:%s) 无法识别的告警类型：%s [FILE:%s, LINE:%d]", m_tabAlarmRule.c_str(), alarm.AlarmID.c_str(), alarm_type.c_str(), __FILE__, __LINE__);
			}

			rs.MoveNext();
		}
		rs.Close();
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
		m_tabAlarmRule.c_str(), alarm.AlarmID.c_str(), alarm.AlarmName.c_str(), alarm_type.c_str(), counter);
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
		//rs.Parameter(1) = kpi_id.c_str();
		//rs.Parameter(1) = comp_res_name.c_str();
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
		throw base::Exception(ADBERR_SEL_COM_RES_DESC, "[DB2] Select compare result description from %s failed! (KPI_ID:%s, DIM_NAME:%s) [CDBException] %s [FILE:%s, LINE:%d]", m_tabDimValue.c_str(), kpi_id.c_str(), comp_res_name.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::SelectYCStatRule(const std::string& kpi_id, std::vector<YCStatInfo>& vec_ycsi) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	std::string st_pri;				// 优先级别
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

			yc_si.stat_id    = (const char*)rs[index++];
			yc_si.stat_name  = (const char*)rs[index++];
			yc_si.statdim_id = (const char*)rs[index++];

			st_pri = (const char*)rs[index++];
			if ( !yc_si.SetStatPriority(st_pri) )
			{
				throw base::Exception(ADBERR_SEL_YC_STATRULE, "[DB2] Select table '%s' failed! (KPI_ID:%s) 无法识别的优先级别：%s [FILE:%s, LINE:%d]", m_tabYCStatRule.c_str(), kpi_id.c_str(), st_pri.c_str(), __FILE__, __LINE__);
			}

			yc_si.stat_sql    = (const char*)rs[index++];
			yc_si.stat_report = (const char*)rs[index++];

			v_yc_si.push_back(yc_si);

			rs.MoveNext();
		}
		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_SEL_YC_STATRULE, "[DB2] Select table '%s' failed! [CDBException] %s [FILE:%s, LINE:%d]", m_tabYCStatRule.c_str(), ex.what(), __FILE__, __LINE__);
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
		throw base::Exception(ADBERR_ALTER_EMPTY_TAB, "[DB2] Alter table [%s] to empty table failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.what(), __FILE__, __LINE__);
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
		throw base::Exception(ADBERR_DEL_FROM_TAB, "[DB2] Delete result data from [%s] failed! [CDBException] %s [FILE:%s, LINE:%d]", tab_name.c_str(), ex.what(), __FILE__, __LINE__);
	}
}

void CAnaDB2::ResultDataInsert(const std::string& db_sql, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception)
{
	XDBO2::CRecordset rs(&m_CDB);
	rs.EnableWarning(true);

	try
	{
		rs.Prepare(db_sql.c_str(), XDBO2::CRecordset::forwardOnly);
		m_pLog->Output("[DB2] Execute insert result data SQL: %s", db_sql.c_str());

		Begin();

		if ( vec2_data.empty() )	// 没有数据，只执行一次SQL
		{
			rs.Execute();
		}
		else
		{
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
		}

		// 最后再提交一次
		Commit();

		rs.Close();
	}
	catch ( const XDBO2::CDBException& ex )
	{
		throw base::Exception(ADBERR_INS_RESULT_DATA, "[DB2] Insert result-data failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

