#include "acquire.h"
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "log.h"
#include "pubtime.h"
#include "pubstr.h"
#include "autodisconnect.h"
#include "cacqdb2.h"
#include "chivethrift.h"
#include "taskinfoutil.h"


Acquire g_Acquire;


Acquire::Acquire()
:m_nHivePort(0)
,m_pAcqDB2(NULL)
,m_pCHive(NULL)
{
	g_pApp = &g_Acquire;
}

Acquire::~Acquire()
{
}

const char* Acquire::Version()
{
	return ("Acquire: Version 1.03.0046 released. Compiled at "__TIME__" on "__DATE__);
}

void Acquire::LoadConfig() throw(base::Exception)
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");
	m_cfg.RegisterItem("HIVE_SERVER", "IP_ADDRESS");
	m_cfg.RegisterItem("HIVE_SERVER", "PORT");

	m_cfg.RegisterItem("TABLE", "TAB_KPI_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_DIM");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_VAL");

	m_cfg.ReadConfig();

	// 数据库配置
	m_sDBName  = m_cfg.GetCfgValue("DATABASE", "DB_NAME");
	m_sUsrName = m_cfg.GetCfgValue("DATABASE", "USER_NAME");
	m_sPasswd  = m_cfg.GetCfgValue("DATABASE", "PASSWORD");

	// Hive服务器配置
	m_sHiveIP   = m_cfg.GetCfgValue("HIVE_SERVER", "IP_ADDRESS");
	m_nHivePort = (int)m_cfg.GetCfgLongVal("HIVE_SERVER", "PORT");
	if ( m_nHivePort <= 0 )
	{
		throw base::Exception(ACQERR_HIVE_PORT_INVALID, "Hive服务器端口无效! (port=%d) [FILE:%s, LINE:%d]", m_nHivePort, __FILE__, __LINE__);
	}

	// Tables
	m_tabKpiRule = m_cfg.GetCfgValue("TABLE", "TAB_KPI_RULE");
	m_tabEtlRule = m_cfg.GetCfgValue("TABLE", "TAB_ETL_RULE");
	m_tabEtlDim  = m_cfg.GetCfgValue("TABLE", "TAB_ETL_DIM");
	m_tabEtlVal  = m_cfg.GetCfgValue("TABLE", "TAB_ETL_VAL");

	m_pLog->Output("Load configuration OK.");
}

void Acquire::Init() throw(base::Exception)
{
	GetParameterTaskInfo(m_ppArgv[4]);

	m_pDB2 = new CAcqDB2(m_sDBName, m_sUsrName, m_sPasswd);
	if ( NULL == m_pDB2 )
	{
		throw base::Exception(ACQERR_INIT_FAILED, "new CAcqDB2 failed: 无法申请到内存空间!");
	}
	m_pAcqDB2 = dynamic_cast<CAcqDB2*>(m_pDB2);

	m_pAcqDB2->SetTabKpiRule(m_tabKpiRule);
	m_pAcqDB2->SetTabEtlRule(m_tabEtlRule);
	m_pAcqDB2->SetTabEtlDim(m_tabEtlDim);
	m_pAcqDB2->SetTabEtlVal(m_tabEtlVal);

	m_pHiveThrift = new CHiveThrift(m_sHiveIP, m_nHivePort);
	if ( NULL == m_pHiveThrift )
	{
		throw base::Exception(ACQERR_INIT_FAILED, "new CHiveThrift failed: 无法申请到内存空间!");
	}
	m_pCHive = dynamic_cast<CHiveThrift*>(m_pHiveThrift);

	m_pCHive->Init();

	m_pLog->Output("Init OK.");
}

void Acquire::Run() throw(base::Exception)
{
	base::AutoDisconnect a_disconn(m_pAcqDB2, m_pCHive);
	a_disconn.Connect();

	AcqTaskInfo task_info;
	SetTaskInfo(task_info);

	FetchTaskInfo(task_info);

	DoDataAcquisition(task_info);
}

void Acquire::GetParameterTaskInfo(const std::string& para) throw(base::Exception)
{
	// 格式：启动批号|指标ID|采集规则ID|...
	std::vector<std::string> vec_str;
	boost::split(vec_str, para, boost::is_any_of("|"));

	if ( vec_str.size() < 3 )
	{
		throw base::Exception(ACQERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法按格式拆分! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	m_sKpiID = vec_str[1];
	boost::trim(m_sKpiID);
	if ( m_sKpiID.empty() )
	{
		throw base::Exception(ACQERR_KPIID_INVALID, "指标ID无效! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_sEtlID = vec_str[2];
	boost::trim(m_sEtlID);
	if ( m_sEtlID.empty() )
	{
		throw base::Exception(ACQERR_ETLID_INVALID, "采集规则ID无效! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] 任务参数信息：指标ID [KPI_ID:%s], 采集规则ID [ETL_ID:%s]", m_sKpiID.c_str(), m_sEtlID.c_str());
}

void Acquire::SetTaskInfo(AcqTaskInfo& info)
{
	info.KpiID     = m_sKpiID;
	info.EtlRuleID = m_sEtlID;
}

void Acquire::FetchTaskInfo(AcqTaskInfo& info) throw(base::Exception)
{
	m_pLog->Output("[Acquire] 查询采集任务规则信息 ...");
	m_pAcqDB2->SelectEtlTaskInfo(info);

	m_pLog->Output("[Acquire] 检查采集任务规则信息 ...");
	CheckTaskInfo(info);
}

void Acquire::CheckTaskInfo(AcqTaskInfo& info) throw(base::Exception)
{
	if ( info.EtlRuleTime.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集时间（周期）为空! 无效! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	if ( info.EtlRuleTarget.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集目标表为空! 无效! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	if ( info.vecEtlRuleDataSrc.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "没有采集数据源! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	size_t src_size = info.vecEtlRuleDataSrc.size();
	size_t dim_size = info.vecEtlRuleDim.size();
	size_t val_size = info.vecEtlRuleVal.size();

	if ( src_size != dim_size )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集数据源个数 (src_size:%lu) 与采集维度个数 (dim_size:%lu) 不一致! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", src_size, dim_size, info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	if ( src_size != val_size )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集数据源个数 (src_size:%lu) 与采集值个数 (val_size:%lu) 不一致! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", src_size, val_size, info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	for ( size_t i = 0; i < dim_size; ++i )
	{
		AcqEtlDim& dim = info.vecEtlRuleDim[i];
		if ( dim.vecEtlDim.empty() )
		{
			throw base::Exception(ACQERR_TASKINFO_INVALID, "采集维度为空! 无效! [DIM_ID:%s] [FILE:%s, LINE:%d]", dim.acqEtlDimID.c_str(), __FILE__, __LINE__);
		}

		AcqEtlVal& val = info.vecEtlRuleVal[i];
		if ( val.vecEtlVal.empty() )
		{
			throw base::Exception(ACQERR_TASKINFO_INVALID, "采集值为空! 无效! [VAL_ID:%s] [FILE:%s, LINE:%d]", val.acqEtlValID.c_str(), __FILE__, __LINE__);
		}
	}

	// 未知的采集条件类型
	if ( AcqTaskInfo::ETLCTYPE_UNKNOWN == info.EtlCondType )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "未知的采集条件类型: ETLCTYPE_UNKNOWN [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
}

void Acquire::DoDataAcquisition(AcqTaskInfo& info) throw(base::Exception)
{
	m_pLog->Output("[Acquire] 分析采集规则 ...");

	std::vector<std::string> vec_field;
	TaskInfo2TargetFields(info, vec_field);

	std::vector<std::string> vec_hivesql;
	TaskInfo2HiveSql(info, vec_hivesql);

	m_pLog->Output("[Acquire] 重建采集目标表 ...");
	m_pCHive->RebuildHiveTable(info.EtlRuleTarget, vec_field);

	m_pLog->Output("[Acquire] 执行数据采集 ...");
	m_pCHive->ExecuteAcqSQL(vec_hivesql);

	m_pLog->Output("[Acquire] 采集数据完成.");
}

void Acquire::TaskInfo2TargetFields(AcqTaskInfo& info, std::vector<std::string>& vec_field) throw(base::Exception)
{
	if ( info.vecEtlRuleDim.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "没有采集维度信息! 无法生成目标表字段! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	if ( info.vecEtlRuleVal.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "没有采集值信息! 无法生成目标表字段! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	std::vector<std::string> v_field;

	// 只取第一组采集维度，作为目标表字段的依据
	AcqEtlDim& first_dim = info.vecEtlRuleDim[0];
	size_t vec_size = first_dim.vecEtlDim.size();
	for ( size_t i = 0; i < vec_size; ++i )
	{
		OneEtlDim& one = first_dim.vecEtlDim[i];

		// 忽略无效维度
		if ( one.EtlDimSeq < 0 )
		{
			m_pLog->Output("[目标表字段] 忽略维度: DIM_ID[%s], DIM_SEQ[%d], DIM_NAME[%s], DIM_SRCNAME[%s], DIM_MEMO[%s]",
				one.EtlDimID.c_str(), one.EtlDimSeq, one.EtlDimName.c_str(), one.EtlDimSrcName.c_str(), one.EtlDimMemo.c_str());
			continue;
		}

		if ( one.EtlDimName.empty() )
		{
			throw base::Exception(ACQERR_TASKINFO_INVALID, "第 [%lu] 个采集维度名称没有设定! [DIM_ID:%s, DIM_SEQ:%d] [FILE:%s, LINE:%d]", (i+1), one.EtlDimID.c_str(), one.EtlDimSeq, __FILE__, __LINE__);
		}

		// 按顺序插入
		v_field.push_back(one.EtlDimName);
	}

	// 只取第一组采集值，作为目标表字段的依据
	AcqEtlVal& first_val = info.vecEtlRuleVal[0];
	vec_size = first_val.vecEtlVal.size();
	for ( size_t i = 0; i < vec_size; ++i )
	{
		OneEtlVal& one = first_val.vecEtlVal[i];

		// 忽略无效值
		if ( one.EtlValSeq < 0 )
		{
			m_pLog->Output("[目标表字段] 忽略值: VAL_ID[%s], VAL_SEQ[%d], VAL_NAME[%s], VAL_SRCNAME[%s], VAL_MEMO[%s]",
				one.EtlValID.c_str(), one.EtlValSeq, one.EtlValName.c_str(), one.EtlValSrcName.c_str(), one.EtlValMemo.c_str());
			continue;
		}

		if ( one.EtlValName.empty() )
		{
			throw base::Exception(ACQERR_TASKINFO_INVALID, "第 [%lu] 个采集值名称没有设定! [VAL_ID:%s, VAL_SEQ:%d] [FILE:%s, LINE:%d]", (i+1), one.EtlValID.c_str(), one.EtlValSeq, __FILE__, __LINE__);
		}

		// 按顺序插入
		v_field.push_back(one.EtlValName);
	}

	v_field.swap(vec_field);
}

void Acquire::TaskInfo2HiveSql(AcqTaskInfo& info, std::vector<std::string>& vec_sql) throw(base::Exception)
{
	switch ( info.EtlCondType )
	{
	case AcqTaskInfo::ETLCTYPE_NONE:		// 不带条件
	case AcqTaskInfo::ETLCTYPE_STRAIGHT:	// 直接条件
		NoneOrStraight2HiveSql(info, vec_sql);
		break;
	case AcqTaskInfo::ETLCTYPE_OUTER_JOIN:	// 外连条件
		OuterJoin2HiveSql(info, vec_sql);
		break;
	case AcqTaskInfo::ETLCTYPE_UNKNOWN:		// 未知类型
	default:
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集规则解析失败：未知的采集条件类型! (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
}

void Acquire::OuterJoin2HiveSql(AcqTaskInfo& info, std::vector<std::string>& vec_sql) throw(base::Exception)
{
	// 分析采集条件
	// 格式：[外连表名]:[关联的维度字段(逗号分隔)]
	std::string& etl_cond = info.EtlCondition;
	boost::trim(etl_cond);

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(etl_cond, ":", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：无法识别的采集条件 [%s] (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", etl_cond.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	const std::string OUTER_TABLE = vec_str[0];		// 外连表名
	if ( OUTER_TABLE.empty() )
	{
		throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：无法识别的采集条件 [%s] (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", etl_cond.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	// 拆分关联字段
	const std::string OUTER_ON = vec_str[1];
	base::PubStr::Str2StrVector(OUTER_ON, ",", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：无法识别的采集条件 [%s] (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", etl_cond.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	std::vector<std::string> v_sql;

	const int OUTER_ON_SIZE = vec_str.size();
	const int SRC_SIZE = info.vecEtlRuleDataSrc.size();
	if ( 1 == SRC_SIZE )		// 单个源数据
	{
		const int NUM_JOIN_ON = TaskInfoUtil::GetNumOfEtlDimJoinOn(info.vecEtlRuleDim[0]);
		if ( OUTER_ON_SIZE != NUM_JOIN_ON )
		{
			throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：采集条件的关联字段数 [%d] 与 维度规则的关联字段数 [%d] (ETLRULE_ID:%s, ETLDIM_ID:%s) [FILE:%s, LINE:%d]", OUTER_ON_SIZE, NUM_JOIN_ON, info.EtlRuleID.c_str(), info.vecEtlRuleDim[0].acqEtlDimID.c_str(), __FILE__, __LINE__);
		}

		const std::string SRC_TABLE = TransDataSrcDate(info.EtlRuleTime, info.vecEtlRuleDataSrc[0]);

		std::string hive_sql = "insert into table " + info.EtlRuleTarget + " ";
		hive_sql += TaskInfoUtil::GetOuterJoinEtlSQL(info.vecEtlRuleDim[0], info.vecEtlRuleVal[0], SRC_TABLE, OUTER_TABLE, vec_str);

		v_sql.push_back(hive_sql);
	}
	else	// 多个源数据
	{
		std::string target_dim_sql = TaskInfoUtil::GetTargetDimSql(info.vecEtlRuleDim[0]);

		// Hive SQL head
		std::string hive_sql = "insert into table " + info.EtlRuleTarget + " select ";
		hive_sql += target_dim_sql + TaskInfoUtil::GetTargetValSql(info.vecEtlRuleVal[0]);
		hive_sql += " from (";

		int num_join_on = 0;
		std::string src_table;

		// Hive SQL body
		for ( int i = 0; i < SRC_SIZE; ++i )
		{
			if ( i != 0 )
			{
				hive_sql += " union all ";
			}

			AcqEtlDim& ref_etl_dim = info.vecEtlRuleDim[i];

			num_join_on = TaskInfoUtil::GetNumOfEtlDimJoinOn(ref_etl_dim);
			if ( OUTER_ON_SIZE != num_join_on )
			{
				throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：采集条件的关联字段数 [%d] 与 维度规则的关联字段数 [%d] (ETLRULE_ID:%s, ETLDIM_ID:%s) [FILE:%s, LINE:%d]", OUTER_ON_SIZE, num_join_on, info.EtlRuleID.c_str(), ref_etl_dim.acqEtlDimID.c_str(), __FILE__, __LINE__);
			}

			src_table = TransDataSrcDate(info.EtlRuleTime, info.vecEtlRuleDataSrc[i]);

			AcqEtlVal& ref_etl_val = info.vecEtlRuleVal[i];

			hive_sql += TaskInfoUtil::GetOuterJoinEtlSQL(ref_etl_dim, ref_etl_val, src_table, OUTER_TABLE, vec_str);
		}

		// Hive SQL tail
		hive_sql += ") TMP group by " + target_dim_sql;

		v_sql.push_back(hive_sql);
	}

	v_sql.swap(vec_sql);
}

void Acquire::NoneOrStraight2HiveSql(AcqTaskInfo& info, std::vector<std::string>& vec_sql)
{
	std::string condition;
	// 是否为直接条件
	if ( AcqTaskInfo::ETLCTYPE_STRAIGHT == info.EtlCondType )
	{
		condition = info.EtlCondition;

		boost::trim(condition);

		std::string head_where = condition.substr(0, 5);
		boost::to_upper(head_where);

		// 加上"where"
		if ( head_where != "WHERE" )
		{
			condition = " where " + condition;
		}
		else
		{
			condition = " " + condition;
		}
	}

	std::vector<std::string> v_sql;

	const int SRC_SIZE = info.vecEtlRuleDataSrc.size();
	if ( 1 == SRC_SIZE )	// 单个源数据
	{
		std::string hive_sql = "insert into table " + info.EtlRuleTarget + " select ";
		hive_sql += TaskInfoUtil::GetEtlDimSql(info.vecEtlRuleDim[0], true) + TaskInfoUtil::GetEtlValSql(info.vecEtlRuleVal[0]);
		hive_sql += " from " + TransDataSrcDate(info.EtlRuleTime, info.vecEtlRuleDataSrc[0]) + condition;
		hive_sql += " group by " + TaskInfoUtil::GetEtlDimSql(info.vecEtlRuleDim[0], false);

		v_sql.push_back(hive_sql);
	}
	else	// 多个源数据
	{
		std::string target_dim_sql = TaskInfoUtil::GetTargetDimSql(info.vecEtlRuleDim[0]);

		// Hive SQL head
		std::string hive_sql = "insert into table " + info.EtlRuleTarget + " select ";
		hive_sql += target_dim_sql + TaskInfoUtil::GetTargetValSql(info.vecEtlRuleVal[0]);
		hive_sql += " from (";

		// Hive SQL body
		std::string tab_alias;
		std::string tab_pre;
		for ( int i = 0; i < SRC_SIZE; ++i )
		{
			if ( i != 0 )
			{
				hive_sql += " union all select ";
			}
			else
			{
				hive_sql += "select ";
			}

			tab_alias = base::PubStr::TabIndex2TabAlias(i);
			tab_pre = tab_alias + ".";
			AcqEtlDim& dim = info.vecEtlRuleDim[i];
			AcqEtlVal& val = info.vecEtlRuleVal[i];

			hive_sql += TaskInfoUtil::GetEtlDimSql(dim, true, tab_pre) + TaskInfoUtil::GetEtlValSql(val, tab_pre);
			hive_sql += " from " + TransDataSrcDate(info.EtlRuleTime, info.vecEtlRuleDataSrc[i]) + " " + tab_alias;
			hive_sql += " group by " + TaskInfoUtil::GetEtlDimSql(dim, false, tab_pre);
		}

		// Hive SQL tail
		hive_sql += ") TMP " + condition + " group by " + target_dim_sql;

		v_sql.push_back(hive_sql);
	}

	v_sql.swap(vec_sql);
}

std::string Acquire::TransDataSrcDate(const std::string& time, const std::string& data_src) throw(base::Exception)
{
	std::string rule_time = time;

	boost::trim(rule_time);
	boost::to_upper(rule_time);

	// 分析是“加”还是“减”
	bool is_plus = true;
	std::vector<std::string> vec_time;
	if ( rule_time.find("+") != std::string::npos )
	{
		is_plus = true;

		boost::split(vec_time, rule_time, boost::is_any_of("+"));
	}
	else if ( rule_time.find("-") != std::string::npos )
	{
		is_plus = false;

		boost::split(vec_time, rule_time, boost::is_any_of("-"));
	}
	else
	{
		throw base::Exception(ACQERR_TRANS_DATASRC_FAILED, "无法识别的采集时间字段(ETLRULE_TIME:%s) [FILE:%s, LINE:%d]", time.c_str(), __FILE__, __LINE__);
	}

	if ( vec_time.size() != 2 )
	{
		throw base::Exception(ACQERR_TRANS_DATASRC_FAILED, "采集时间字段(ETLRULE_TIME:%s) 格式错误! [FILE:%s, LINE:%d]", time.c_str(), __FILE__, __LINE__);
	}

	// 时间偏移量数值转换
	unsigned int time_off = 0;
	try
	{
		boost::trim(vec_time[1]);
		time_off = boost::lexical_cast<unsigned int>(vec_time[1]);
	}
	catch ( boost::bad_lexical_cast& ex )
	{
		throw base::Exception(ACQERR_TRANS_DATASRC_FAILED, "(ETLRULE_TIME:%s) 采集时间偏移量转换失败: %s [FILE:%s, LINE:%d]", time.c_str(), vec_time[1].c_str(), __FILE__, __LINE__);
	}

	std::string be_replace;
	std::string confirm_time;
	std::string& time_flag = vec_time[0];
	boost::trim(time_flag);
	if ( "DAY" == time_flag )
	{
		if ( is_plus )
		{
			confirm_time = base::PubTime::DateNowPlusDays(time_off);
		}
		else
		{
			confirm_time = base::PubTime::DateNowMinusDays(time_off);
		}

		be_replace = "YYYYMMDD";
	}
	else if ( "MON" == time_flag )
	{
		if ( is_plus )
		{
			confirm_time = base::PubTime::DateNowPlusMonths(time_off);
		}
		else
		{
			confirm_time = base::PubTime::DateNowMinusMonths(time_off);
		}

		be_replace = "YYYYMM";
	}
	else
	{
		throw base::Exception(ACQERR_TRANS_DATASRC_FAILED, "(ETLRULE_TIME:%s) 无法识别的采集时间标识: %s [FILE:%s, LINE:%d]", time.c_str(), time_flag.c_str(), __FILE__, __LINE__);
	}

	std::string new_datasrc = data_src;

	boost::trim(new_datasrc);
	boost::to_upper(new_datasrc);

	// 找到时间标记才进行替换
	size_t t_pos = new_datasrc.find(be_replace);
	if ( t_pos != std::string::npos )
	{
		new_datasrc.replace(t_pos, be_replace.size(), confirm_time);
	}
	//if ( std::string::npos == t_pos )
	//{
	//	throw base::Exception(ACQERR_TRANS_DATASRC_FAILED, "在源数据表名 (%s) 中找不到时间标记: %s [FILE:%s, LINE:%d]", data_src.c_str(), be_replace.c_str(), __FILE__, __LINE__);
	//}

	return new_datasrc;
}

