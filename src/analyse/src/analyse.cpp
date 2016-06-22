#include "analyse.h"
#include <vector>
#include <set>
#include <map>
#include "log.h"
#include "autodisconnect.h"
#include "pubstr.h"
#include "pubtime.h"
#include "canadb2.h"
#include "canahive.h"
#include "taskinfoutil.h"
#include "alarmevent.h"
#include "alarmfluctuate.h"
#include "alarmratio.h"


Analyse g_Analyse;


Analyse::Analyse()
//:m_nHivePort(0)
:m_pAnaDB2(NULL)
,m_pAnaHive(NULL)
{
	g_pApp = &g_Analyse;
}

Analyse::~Analyse()
{
}

const char* Analyse::Version()
{
	return ("Analyse: Version 1.16.0094 released. Compiled at "__TIME__" on "__DATE__);
}

void Analyse::LoadConfig() throw(base::Exception)
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");
	//m_cfg.RegisterItem("HIVE_SERVER", "IP_ADDRESS");
	//m_cfg.RegisterItem("HIVE_SERVER", "PORT");
	//m_cfg.RegisterItem("HIVE_SERVER", "USERNAME");
	//m_cfg.RegisterItem("HIVE_SERVER", "PASSWORD");
	m_cfg.RegisterItem("HIVE_SERVER", "ZK_QUORUM");
	m_cfg.RegisterItem("HIVE_SERVER", "KRB5_CONF");
	m_cfg.RegisterItem("HIVE_SERVER", "USR_KEYTAB");
	m_cfg.RegisterItem("HIVE_SERVER", "PRINCIPAL");
	m_cfg.RegisterItem("HIVE_SERVER", "JAAS_CONF");
	m_cfg.RegisterItem("HIVE_SERVER", "LOAD_JAR_PATH");

	m_cfg.RegisterItem("TABLE", "TAB_KPI_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_KPI_COLUMN");
	m_cfg.RegisterItem("TABLE", "TAB_DIM_VALUE");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_DIM");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_VAL");
	m_cfg.RegisterItem("TABLE", "TAB_ANA_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_EVENT");
	m_cfg.RegisterItem("TABLE", "TAB_DICT_CHANNEL");
	m_cfg.RegisterItem("TABLE", "TAB_DICT_CITY");

	m_cfg.ReadConfig();

	// 数据库配置
	m_sDBName  = m_cfg.GetCfgValue("DATABASE", "DB_NAME");
	m_sUsrName = m_cfg.GetCfgValue("DATABASE", "USER_NAME");
	m_sPasswd  = m_cfg.GetCfgValue("DATABASE", "PASSWORD");

	// Hive服务器配置
	m_zk_quorum  = m_cfg.GetCfgValue("HIVE_SERVER", "ZK_QUORUM");
	m_krb5_conf  = m_cfg.GetCfgValue("HIVE_SERVER", "KRB5_CONF");
	m_usr_keytab = m_cfg.GetCfgValue("HIVE_SERVER", "USR_KEYTAB");
	m_principal  = m_cfg.GetCfgValue("HIVE_SERVER", "PRINCIPAL");
	m_jaas_conf  = m_cfg.GetCfgValue("HIVE_SERVER", "JAAS_CONF");
	//m_sHiveIP   = m_cfg.GetCfgValue("HIVE_SERVER", "IP_ADDRESS");
	//m_nHivePort = (int)m_cfg.GetCfgLongVal("HIVE_SERVER", "PORT");
	//if ( m_nHivePort <= 0 )
	//{
	//	throw base::Exception(ANAERR_HIVE_PORT_INVALID, "Hive服务器端口无效! (port=%d) [FILE:%s, LINE:%d]", m_nHivePort, __FILE__, __LINE__);
	//}
	//m_sHiveUsr = m_cfg.GetCfgValue("HIVE_SERVER", "USERNAME");
	//m_sHivePwd = m_cfg.GetCfgValue("HIVE_SERVER", "PASSWORD");
	m_sLoadJarPath = m_cfg.GetCfgValue("HIVE_SERVER", "LOAD_JAR_PATH");

	// Tables
	m_tabKpiRule     = m_cfg.GetCfgValue("TABLE", "TAB_KPI_RULE");
	m_tabKpiColumn   = m_cfg.GetCfgValue("TABLE", "TAB_KPI_COLUMN");
	m_tabDimValue    = m_cfg.GetCfgValue("TABLE", "TAB_DIM_VALUE");
	m_tabEtlRule     = m_cfg.GetCfgValue("TABLE", "TAB_ETL_RULE");
	m_tabEtlDim      = m_cfg.GetCfgValue("TABLE", "TAB_ETL_DIM");
	m_tabEtlVal      = m_cfg.GetCfgValue("TABLE", "TAB_ETL_VAL");
	m_tabAnaRule     = m_cfg.GetCfgValue("TABLE", "TAB_ANA_RULE");
	m_tabAlarmRule   = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_RULE");
	m_tabAlarmEvent  = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_EVENT");
	m_tabDictChannel = m_cfg.GetCfgValue("TABLE", "TAB_DICT_CHANNEL");
	m_tabDictCity    = m_cfg.GetCfgValue("TABLE", "TAB_DICT_CITY");

	m_pLog->Output("Load configuration OK.");
}

void Analyse::Init() throw(base::Exception)
{
	GetParameterTaskInfo(m_ppArgv[4]);

	m_pDB2 = new CAnaDB2(m_sDBName, m_sUsrName, m_sPasswd);
	if ( NULL == m_pDB2 )
	{
		throw base::Exception(ANAERR_INIT_FAILED, "new CAnaDB2 failed: 无法申请到内存空间!");
	}
	m_pAnaDB2 = dynamic_cast<CAnaDB2*>(m_pDB2);

	m_pAnaDB2->SetTabKpiRule(m_tabKpiRule);
	m_pAnaDB2->SetTabKpiColumn(m_tabKpiColumn);
	m_pAnaDB2->SetTabDimValue(m_tabDimValue);
	m_pAnaDB2->SetTabEtlRule(m_tabEtlRule);
	m_pAnaDB2->SetTabEtlDim(m_tabEtlDim);
	m_pAnaDB2->SetTabEtlVal(m_tabEtlVal);
	m_pAnaDB2->SetTabAnaRule(m_tabAnaRule);
	m_pAnaDB2->SetTabAlarmRule(m_tabAlarmRule);
	m_pAnaDB2->SetTabAlarmEvent(m_tabAlarmEvent);
	m_pAnaDB2->SetTabDictChannel(m_tabDictChannel);
	m_pAnaDB2->SetTabDictCity(m_tabDictCity);

	//m_pHive = new CAnaHive(m_sHiveIP, m_nHivePort, m_sHiveUsr, m_sHivePwd);
	m_pHive = new CAnaHive();
	if ( NULL == m_pHive )
	{
		throw base::Exception(ANAERR_INIT_FAILED, "new CAnaHive failed: 无法申请到内存空间!");
	}
	m_pAnaHive = dynamic_cast<CAnaHive*>(m_pHive);

	m_pAnaHive->Init(m_sLoadJarPath);
	m_pAnaHive->SetZooKeeperQuorum(m_zk_quorum);
	m_pAnaHive->SetKrb5Conf(m_krb5_conf);
	m_pAnaHive->SetUserKeytab(m_usr_keytab);
	m_pAnaHive->SetPrincipal(m_principal);
	m_pAnaHive->SetJaasConf(m_jaas_conf);

	m_pLog->Output("Init OK.");
}

void Analyse::Run() throw(base::Exception)
{
	base::AutoDisconnect a_disconn(new base::HiveDB2Connector(m_pAnaDB2, m_pAnaHive));
	a_disconn.Connect();

	AnaTaskInfo task_info;
	SetTaskInfo(task_info);

	FetchTaskInfo(task_info);

	DoDataAnalyse(task_info);
}

void Analyse::GetParameterTaskInfo(const std::string& para) throw(base::Exception)
{
	// 格式：启动批号:指标ID:分析规则ID:...
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(para, ":", vec_str);

	if ( vec_str.size() < 3 )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法按格式拆分! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	m_sKpiID = vec_str[1];
	base::PubStr::Trim(m_sKpiID);
	if ( m_sKpiID.empty() )
	{
		throw base::Exception(ANAERR_KPIID_INVALID, "指标ID无效! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_sAnaID = vec_str[2];
	base::PubStr::Trim(m_sAnaID);
	if ( m_sAnaID.empty() )
	{
		throw base::Exception(ANAERR_ANAID_INVALID, "分析规则ID无效! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[Analyse] 任务参数信息：指标ID [KPI_ID:%s], 分析规则ID [ANA_ID:%s]", m_sKpiID.c_str(), m_sAnaID.c_str());
}

void Analyse::GetAnaDBInfo(AnaTaskInfo& t_info) throw(base::Exception)
{
	m_dbinfo.time_stamp = false;
	std::string str_tmp;

	std::vector<std::string> v_fields;
	int v_size = t_info.vecKpiDimCol.size();
	for ( int i = 0; i < v_size; ++i )
	{
		KpiColumn& col = t_info.vecKpiDimCol[i];

		if ( col.DBName.empty() )
		{
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "维度字段名为空! 无效! (KPI_ID=%s, COL_TYPE=CTYPE_DIM, COL_SEQ=%d) [FILE:%s, LINE:%d]", col.KpiID.c_str(), col.ColSeq, __FILE__, __LINE__);
		}

		if ( col.ColSeq < 0 )
		{
			if ( -1 == col.ColSeq )		// ColType为CTYPE_DIM时, col.ColSeq = -1 表示时间戳
			{
				if ( !m_dbinfo.time_stamp )
				{
					m_dbinfo.time_stamp = true;
					str_tmp = col.DBName;
				}
				else
				{
					throw base::Exception(ANAERR_GET_DBINFO_FAILED, "时间戳维度字段重复设置: KPI_ID=%s, COL_TYPE=CTYPE_DIM, COL_SEQ=%d, DB_NAME=%s [FILE:%s, LINE:%d]", col.KpiID.c_str(), col.ColSeq, col.DBName.c_str(), __FILE__, __LINE__);
				}
			}
			else
			{
				throw base::Exception(ANAERR_GET_DBINFO_FAILED, "无法识别的维度字段序号: %d (KPI_ID=%s, COL_TYPE=CTYPE_DIM, DB_NAME=%s) [FILE:%s, LINE:%d]", col.ColSeq, col.KpiID.c_str(), col.DBName.c_str(), __FILE__, __LINE__);
			}
		}
		else
		{
			v_fields.push_back(col.DBName);
		}
	}

	v_size = t_info.vecKpiValCol.size();
	for ( int i = 0; i < v_size; ++i )
	{
		KpiColumn& col = t_info.vecKpiValCol[i];

		if ( col.DBName.empty() )
		{
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "值字段名为空! 无效! (KPI_ID=%s, COL_TYPE=CTYPE_VAL, COL_SEQ=%d) [FILE:%s, LINE:%d]", col.KpiID.c_str(), col.ColSeq, __FILE__, __LINE__);
		}

		if ( col.ColSeq < 0 )
		{
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "无法识别的值字段序号: %d (KPI_ID=%s, COL_TYPE=CTYPE_VAL, DB_NAME=%s) [FILE:%s, LINE:%d]", col.ColSeq, col.KpiID.c_str(), col.DBName.c_str(), __FILE__, __LINE__);
		}

		v_fields.push_back(col.DBName);
	}

	// 加时间戳
	if ( m_dbinfo.time_stamp )
	{
		v_fields.push_back(str_tmp);
	}

	// 按表的类型生成最终目标表名
	GenerateTableNameByType(t_info);

	// 组织数据库入库SQL语句
	m_dbinfo.db2_sql = "insert into " + m_dbinfo.target_table + "(";

	str_tmp.clear();
	v_size = v_fields.size();
	for ( int i = 0; i < v_size; ++i )
	{
		if ( i != 0 )
		{
			m_dbinfo.db2_sql += ", " + v_fields[i];

			str_tmp += ", ?";
		}
		else
		{
			m_dbinfo.db2_sql += v_fields[i];

			str_tmp += "?";
		}
	}

	m_dbinfo.db2_sql += ") values(" + str_tmp + ")";

	v_fields.swap(m_dbinfo.vec_fields);
}

void Analyse::FetchHiveSource(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	std::vector<std::vector<std::string> > vec2_fields;

	const int SQL_SIZE = vec_hivesql.size();
	for ( int i = 0; i < SQL_SIZE; ++i )
	{
		m_pAnaHive->FetchSourceData(vec_hivesql[i], vec2_fields);

		m_pLog->Output("[Analyse] 获取第 %d 组源数据记录数：%llu", (i+1), vec2_fields.size());
		base::PubStr::VVVectorSwapPushBack(m_v3HiveSrcData, vec2_fields);
	}
}

void Analyse::UpdateDimValue(AnaTaskInfo& info)
{
	CollectDimVal(info);

	m_pAnaDB2->SelectDimValue(info.KpiID, m_DVDiffer);
	m_pLog->Output("[Analyse] 从数据库中获取指标 (KPI_ID:%s) 的维度取值范围, size: %llu", info.KpiID.c_str(), m_DVDiffer.GetDBDimValSize());

	std::vector<DimVal> vec_diff_dv;
	m_DVDiffer.GetDimValDiff(vec_diff_dv);

	const size_t VEC_DV = vec_diff_dv.size();
	m_pLog->Output("[Analyse] 需要更新的维度取值数：%llu", VEC_DV);

	if ( VEC_DV > 0 )
	{
		m_pAnaDB2->InsertNewDimValue(vec_diff_dv);
		m_pLog->Output("[Analyse] 更新维度取值范围成功！");
	}
	else
	{
		m_pLog->Output("[Analyse] 无需更新维度取值范围.");
	}
}

//void Analyse::AddAnalysisCondition(AnalyseRule& ana_rule, std::vector<std::string>& vec_sql)
//{
//	const std::string CONDITION = TaskInfoUtil::GetStraightAnaCondition(ana_rule.AnaCondType, ana_rule.AnaCondition, false);
//
//	const int VEC_SIZE = vec_sql.size();
//	for ( int i = 0; i < VEC_SIZE; ++i )
//	{
//		std::string& ref_sql = vec_sql[i];
//
//		TaskInfoUtil::AddConditionSql(ref_sql, CONDITION);
//	}
//}

void Analyse::SetTaskInfo(AnaTaskInfo& info)
{
	info.KpiID         = m_sKpiID;
	info.AnaRule.AnaID = m_sAnaID;
}

void Analyse::FetchTaskInfo(AnaTaskInfo& info) throw(base::Exception)
{
	m_pLog->Output("[Analyse] 查询分析任务规则信息 ...");
	m_pAnaDB2->SelectAnaTaskInfo(info);

	m_pLog->Output("[Analyse] 检查分析任务规则信息 ...");
	CheckAnaTaskInfo(info);

	m_pLog->Output("[Analyse] 获取渠道、地市统一编码信息 ...");
	FetchUniformCode();
}

void Analyse::CheckAnaTaskInfo(AnaTaskInfo& info) throw(base::Exception)
{
	if ( AnaTaskInfo::TABTYPE_UNKNOWN == info.ResultType )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "未知的分析结果表类型! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( info.TableName.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "分析结果表为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( AnalyseRule::ANATYPE_UNKNOWN == info.AnaRule.AnaType )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "未知的分析规则类型! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	// 分析规则表达式可能为空！
	//if ( info.AnaRule.AnaExpress.empty() )
	//{
	//	throw base::Exception(ANAERR_TASKINFO_INVALID, "分析规则表达式为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	//}

	if ( info.vecEtlRule.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有采集规则信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( info.vecKpiDimCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标维度信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( info.vecKpiValCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标值信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	//if ( AnalyseRule::ACTYPE_UNKNOWN == info.AnaRule.AnaCondType )
	//{
	//	throw base::Exception(ANAERR_TASKINFO_INVALID, "未知的分析条件类型! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	//}
}

void Analyse::FetchUniformCode() throw(base::Exception)
{
	m_pLog->Output("[Analyse] Fetch channel uniform code ...");
	std::vector<ChannelUniformCode> vec_channel_uni_code;
	m_pAnaDB2->SelectChannelUniformCode(vec_channel_uni_code);

	m_pLog->Output("[Analyse] Fetch channel uniform code, size: %llu", vec_channel_uni_code.size());
	m_UniCodeTransfer.InputChannelUniformCode(vec_channel_uni_code);

	m_pLog->Output("[Analyse] Fetch city uniform code ...");
	std::vector<CityUniformCode> vec_city_uni_code;
	m_pAnaDB2->SelectCityUniformCode(vec_city_uni_code);

	m_pLog->Output("[Analyse] Fetch city uniform code, size: %llu", vec_city_uni_code.size());
	m_UniCodeTransfer.InputCityUniformCode(vec_city_uni_code);
}

void Analyse::DoDataAnalyse(AnaTaskInfo& t_info) throw(base::Exception)
{
	m_pLog->Output("[Analyse] 解析分析规则 ...");

	std::vector<std::string> vec_hivesql;
	AnalyseRules(t_info, vec_hivesql);

	m_pLog->Output("[Analyse] 获取Hive源数据 ...");
	FetchHiveSource(vec_hivesql);

	m_pLog->Output("[Analyse] 分析源数据 ...");
	AnalyseSourceData(t_info);

	m_pLog->Output("[Analyse] 生成结果数据 ...");
	StoreResult(t_info);

	m_pLog->Output("[Analyse] 告警判断 ...");
	AlarmJudgement(t_info);

	m_pLog->Output("[Analyse] 更新维度取值范围 ...");
	UpdateDimValue(t_info);
}

void Analyse::AnalyseRules(AnaTaskInfo& t_info, std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	std::string& ref_exp = t_info.AnaRule.AnaExpress;
	base::PubStr::Trim(ref_exp);

	// 分析规则类型
	switch ( t_info.AnaRule.AnaType )
	{
	case AnalyseRule::ANATYPE_SUMMARY_COMPARE:		// 汇总对比
		m_pLog->Output("[Analyse] 分析规则类型：汇总对比 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetSummaryCompareHiveSQL(t_info, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_DETAIL_COMPARE:		// 明细对比
		m_pLog->Output("[Analyse] 分析规则类型：明细对比 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetDetailCompareHiveSQL(t_info, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_STATISTICS:			// 一般统计
		m_pLog->Output("[Analyse] 分析规则类型：一般统计 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetStatisticsHiveSQL(t_info, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_REPORT_STATISTICS:	// 报表统计
		m_pLog->Output("[Analyse] 分析规则类型：报表统计 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetReportStatisticsHiveSQL(t_info, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_HIVE_SQL:				// 可执行的HIVE SQL语句
		m_pLog->Output("[Analyse] 分析规则类型：可执行的HIVE SQL语句 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		// 分析表达式，即是可执行的Hive SQL语句（可多个，以分号分隔）
		SplitHiveSqlExpress(ref_exp, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_UNKNOWN:				// 未知类型
	default:
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "无法识别的分析规则类型: ANATYPE_UNKNOWN (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	// 生成数据库[DB2]信息
	GetAnaDBInfo(t_info);
}

// 暂时只支持两组数据的汇总对比
void Analyse::GetSummaryCompareHiveSQL(AnaTaskInfo& t_info, std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckDualEtlRule(t_info.vecEtlRule) )
	{
	case -1:	// 采集规则个数不够
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "只有 %lu 份采集结果数据，无法进行汇总对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.vecEtlRule.size(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "采集规则的维度size不一致，无法进行汇总对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "采集规则的值size不一致，无法进行汇总对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 格式样例: A-B, diff1:diff2
	std::string& ana_exp = t_info.AnaRule.AnaExpress;

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(ana_exp, ",", vec_str);
	if ( vec_str.size() != 2 )
	{
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	int first_index  = -1;
	int second_index = -1;
	DetermineDataGroup(vec_str[0], first_index, second_index);

	std::string diff_str = vec_str[1];
	base::PubStr::Str2StrVector(diff_str, ":", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	const int VAL_SIZE_1 = t_info.vecEtlRule[0].vecEtlVal.size();
	std::set<int> set_diff;			// 用于查重
	std::vector<int> vec_col;
	const int DIFF_SIZE = vec_str.size();
	for ( int i = 0; i < DIFF_SIZE; ++i )
	{
		std::string& ref_str = vec_str[i];

		base::PubStr::Upper(ref_str);
		size_t pos = ref_str.find("DIFF");
		if ( std::string::npos == pos )
		{
			throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}

		int diff_no = -1;
		if ( !base::PubStr::T1TransT2(ref_str.substr(pos+4), diff_no) )
		{
			throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：[%s] 转换失败! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_str.c_str(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}

		if ( diff_no < 1 || diff_no > VAL_SIZE_1 )
		{
			throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：列值 [%d] 越界! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", diff_no, t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}

		if ( set_diff.find(diff_no) != set_diff.end() )
		{
			throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：列值 [%d] 已经存在! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", diff_no, t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}

		set_diff.insert(diff_no);

		// 表达式中DIFF从1开始，而列序号是从0开始的
		vec_col.push_back(diff_no-1);
	}

	std::vector<std::string> v_hive_sql;

	OneEtlRule& first_one  = t_info.vecEtlRule[first_index];
	OneEtlRule& second_one = t_info.vecEtlRule[second_index];

	//// 指定分析条件
	//const std::string CONDITION     = TaskInfoUtil::GetStraightAnaCondition(t_info.AnaRule.AnaCondType, t_info.AnaRule.AnaCondition, false);
	//const std::string ADD_CONDITION = TaskInfoUtil::GetStraightAnaCondition(t_info.AnaRule.AnaCondType, t_info.AnaRule.AnaCondition, true);

	// 1) 汇总：对平的Hive SQL语句
	std::string hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col);
	hive_sql += ", '对平' from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += " and " + TaskInfoUtil::GetCompareEqualValsByCol(first_one, second_one, vec_col) + ")";
	//TaskInfoUtil::AddConditionSql(hive_sql, CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 2) 汇总：有差异的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col);
	hive_sql += ", '有差异' from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetCompareUnequalValsByCol(first_one, second_one, vec_col);
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	v_hive_sql.swap(vec_hivesql);
}

// 暂时只支持两组数据的明细对比
void Analyse::GetDetailCompareHiveSQL(AnaTaskInfo& t_info, std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckDualEtlRule(t_info.vecEtlRule) )
	{
	case -1:	// 采集规则个数不够
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "只有 %lu 份采集结果数据，无法进行明细对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.vecEtlRule.size(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "采集规则的维度size不一致，无法进行明细对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "采集规则的值size不一致，无法进行明细对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 格式样例: A-B
	// 暂时只支持这一种表达式："A-B"
	std::string& ana_exp = t_info.AnaRule.AnaExpress;

	int first_index  = -1;
	int second_index = -1;
	DetermineDataGroup(ana_exp, first_index, second_index);

	std::vector<std::string> v_hive_sql;

	OneEtlRule& first_one  = t_info.vecEtlRule[first_index];
	OneEtlRule& second_one = t_info.vecEtlRule[second_index];

	//// 指定分析条件
	//const std::string CONDITION     = TaskInfoUtil::GetStraightAnaCondition(t_info.AnaRule.AnaCondType, t_info.AnaRule.AnaCondition, false);
	//const std::string ADD_CONDITION = TaskInfoUtil::GetStraightAnaCondition(t_info.AnaRule.AnaCondType, t_info.AnaRule.AnaCondition, true);

	// 1) 明细：对平的Hive SQL语句
	std::string hive_sql = "select " + TaskInfoUtil::GetCompareFields(first_one, second_one);
	hive_sql += ", '对平' from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += " and " + TaskInfoUtil::GetCompareEqualVals(first_one, second_one) + ")";
	//TaskInfoUtil::AddConditionSql(hive_sql, CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 2) 明细：有差异的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFields(first_one, second_one);
	hive_sql += ", '有差异' from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetCompareUnequalVals(first_one, second_one);
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 3) 明细：左有右无的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFields(first_one, second_one);
	hive_sql += ", '左有右无' from " + first_one.TargetPatch + " a left outer join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetOneRuleValsNull(second_one, "b.");
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 4) 明细：左无右有的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFields(second_one, first_one, true);
	hive_sql += ", '左无右有' from " + second_one.TargetPatch + " b left outer join " + first_one.TargetPatch;
	hive_sql += " a on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetOneRuleValsNull(first_one, "a.");
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	v_hive_sql.swap(vec_hivesql);
}

void Analyse::GetStatisticsHiveSQL(AnaTaskInfo& t_info, std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckPluralEtlRule(t_info.vecEtlRule) )
	{
	case -1:	// 没有采集规则
		throw base::Exception(ANAERR_GET_STATISTICS_FAILED, "没有采集结果数据，无法进行一般统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.vecEtlRule.size(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_STATISTICS_FAILED, "采集规则的维度size不一致，无法进行一般统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_STATISTICS_FAILED, "采集规则的值size不一致，无法进行一般统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 当有多份采集结果数据时，采用联合语句
	bool is_union_all = (t_info.vecEtlRule.size() > 1);

	std::string& ana_exp = t_info.AnaRule.AnaExpress;
	if ( ana_exp.empty() )	// 没有指定表达式，则取全量数据
	{
		TaskInfoUtil::GetEtlStatisticsSQLs(t_info.vecEtlRule, vec_hivesql, is_union_all);
	}
	else
	{
		GetStatisticsHiveSQLBySet(t_info, vec_hivesql, is_union_all);
	}

	//AddAnalysisCondition(t_info.AnaRule, vec_hivesql);
}

void Analyse::GetReportStatisticsHiveSQL(AnaTaskInfo& t_info, std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckPluralEtlRule(t_info.vecEtlRule) )
	{
	case -1:	// 没有采集规则
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "没有采集结果数据，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.vecEtlRule.size(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "采集规则的维度size不一致，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "采集规则的值size不一致，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 采集规则的值是否唯一
	// 由于上面已经确保了值的size都是一致的，所以检查第一组就可以了
	if ( t_info.vecEtlRule[0].vecEtlVal.size() != 1 )
	{
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "采集规则的值不唯一，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	std::string& ana_exp = t_info.AnaRule.AnaExpress;
	if ( ana_exp.empty() )	// 没有指定表达式，则取全量数据
	{
		TaskInfoUtil::GetEtlStatisticsSQLs(t_info.vecEtlRule, vec_hivesql, false);
	}
	else
	{
		GetStatisticsHiveSQLBySet(t_info, vec_hivesql, false);
	}

	//AddAnalysisCondition(t_info.AnaRule, vec_hivesql);
}

void Analyse::GetStatisticsHiveSQLBySet(AnaTaskInfo& t_info, std::vector<std::string>& vec_hivesql, bool union_all) throw(base::Exception)
{
	std::string& ana_exp = t_info.AnaRule.AnaExpress;

	std::set<int> set_int;
	const int RESULT = base::PubStr::Express2IntSet(ana_exp, set_int);

	if ( -1 == RESULT )		// 转换失败
	{
		throw base::Exception(ANAERR_GET_STAT_BY_SET_FAILED, "分析规则解析失败：分析规则表达式转换失败! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}
	else if ( 1 == RESULT )		// 数据重复
	{
		throw base::Exception(ANAERR_GET_STAT_BY_SET_FAILED, "分析规则解析失败：分析规则表达式存在重复数据! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}
	else
	{
		const int MAX_ETL_RULE_SIZE = t_info.vecEtlRule.size();

		// 检查合法性
		for ( std::set<int>::iterator it = set_int.begin(); it != set_int.end(); ++it )
		{
			const int& VAL = *it;

			if ( VAL < 1 || VAL > MAX_ETL_RULE_SIZE )
			{
				throw base::Exception(ANAERR_GET_STAT_BY_SET_FAILED, "分析规则解析失败：不存在指定的采集组 [%d] ! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", VAL, t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}
		}

		TaskInfoUtil::GetEtlStatisticsSQLsBySet(t_info.vecEtlRule, set_int, vec_hivesql, union_all);
	}
}

void Analyse::SplitHiveSqlExpress(const std::string& exp_sqls, std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	std::string exp = base::PubStr::TrimB(exp_sqls);
	if ( exp.empty() )
	{
		throw base::Exception(ANAERR_SPLIT_HIVESQL_FAILED, "可执行Hive SQL语句为空! 无法拆分! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 删除最后的分号
	const int LAST_INDEX = exp.size() - 1;
	if ( ';' == exp[LAST_INDEX] )
	{
		exp.erase(LAST_INDEX);
	}

	std::vector<std::string> v_hive_sql;
	base::PubStr::Str2StrVector(exp, ";", v_hive_sql);

	if ( v_hive_sql.empty() )
	{
		throw base::Exception(ANAERR_SPLIT_HIVESQL_FAILED, "拆分失败：没有可执行Hive SQL语句! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	v_hive_sql.swap(vec_hivesql);
}

void Analyse::DetermineDataGroup(const std::string& exp, int& first, int& second) throw(base::Exception)
{
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(exp, "-", vec_str);
	if ( vec_str.size() != 2 )
	{
		throw base::Exception(ANAERR_DETERMINE_GROUP_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] [FILE:%s, LINE:%d]", exp.c_str(), __FILE__, __LINE__);
	}

	std::string& first_group  = vec_str[0];
	std::string& second_group = vec_str[1];

	base::PubStr::Upper(first_group);
	base::PubStr::Upper(second_group);

	int first_index  = -1;
	int second_index = -1;

	// 确定数据组
	if ( first_group.size() != 1 )
	{
		throw base::Exception(ANAERR_DETERMINE_GROUP_FAILED, "分析规则表达式解析失败：不支持的数据组 [%s] [FILE:%s, LINE:%d]", first_group.c_str(), __FILE__, __LINE__);
	}
	else
	{
		first_index = first_group[0] - 'A';

		// 是否为无效组
		if ( first_index < 0 || first_index > 1 )
		{
			throw base::Exception(ANAERR_DETERMINE_GROUP_FAILED, "分析规则表达式解析失败：不支持的数据组 [%s] [FILE:%s, LINE:%d]", first_group.c_str(), __FILE__, __LINE__);
		}
	}

	if ( second_group.size() != 1 )
	{
		throw base::Exception(ANAERR_DETERMINE_GROUP_FAILED, "分析规则表达式解析失败：不支持的数据组 [%s] [FILE:%s, LINE:%d]", second_group.c_str(), __FILE__, __LINE__);
	}
	else
	{
		second_index = second_group[0] - 'A';

		// 是否为无效组
		if ( second_index < 0 || second_index > 1 )
		{
			throw base::Exception(ANAERR_DETERMINE_GROUP_FAILED, "分析规则表达式解析失败：不支持的数据组 [%s] [FILE:%s, LINE:%d]", second_group.c_str(), __FILE__, __LINE__);
		}
	}

	if ( first_index == second_index )
	{
		throw base::Exception(ANAERR_DETERMINE_GROUP_FAILED, "分析规则表达式解析失败：数据组 [%s] 重复! [FILE:%s, LINE:%d]", first_group.c_str(), __FILE__, __LINE__);
	}

	first  = first_index;
	second = second_index;
}

void Analyse::GenerateTableNameByType(AnaTaskInfo& info) throw(base::Exception)
{
	AnaTaskInfo::ResultTableType& type = info.ResultType;

	// 取第一个采集规则的采集时间
	std::string& ref_etltime = info.vecEtlRule[0].EtlTime;

	base::PubTime::DATE_TYPE d_type;
	if ( !base::PubTime::DateApartFromNow(ref_etltime, d_type, m_dbinfo.date_time) )
	{
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "采集时间转换失败！无法识别的采集时间表达式：%s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_etltime.c_str(), info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	std::string tab_name = base::PubStr::TrimB(info.TableName);

	switch ( type )
	{
	case AnaTaskInfo::TABTYPE_COMMON:		// 普通表
		// Do nothing
		break;
	case AnaTaskInfo::TABTYPE_DAY:			// 天表
		if ( d_type > base::PubTime::DT_DAY )
		{
			throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "采集时间类型与结果表类型无法匹配！采集时间类型为：%s，结果表类型为：DAY_TABLE (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", base::PubTime::DateType2String(d_type).c_str(), info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}
		else
		{
			tab_name = tab_name + "_" + m_dbinfo.date_time;
		}
		break;
	case AnaTaskInfo::TABTYPE_MONTH:		// 月表
		if ( d_type > base::PubTime::DT_MONTH )
		{
			throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "采集时间类型与结果表类型无法匹配！采集时间类型为：%s，结果表类型为：MONTH_TABLE (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", base::PubTime::DateType2String(d_type).c_str(), info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}
		else
		{
			tab_name = tab_name + "_" + m_dbinfo.date_time.substr(0, 6);
		}
		break;
	case AnaTaskInfo::TABTYPE_YEAR:			// 年表
		tab_name = tab_name + "_" + m_dbinfo.date_time.substr(0, 4);
		break;
	case AnaTaskInfo::TABTYPE_UNKNOWN:		// 未知类型
	default:
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "无法识别的目标表类型: TABTYPE_UNKNOWN (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	m_dbinfo.target_table = tab_name;
	m_pLog->Output("[Analyse] 最终目标表名：%s", tab_name.c_str());
}

void Analyse::AnalyseSourceData(AnaTaskInfo& t_info) throw(base::Exception)
{
	// 报表统计
	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == t_info.AnaRule.AnaType )
	{
		m_pLog->Output("[Analyse] 报表统计类型数据转换 ...");

		m_pLog->Output("[Analyse] 转换前, 数据大小为: %llu", base::PubStr::CalcVVVectorStr(m_v3HiveSrcData));
		TransSrcDataToReportStatData();
		m_pLog->Output("[Analyse] 转换后, 数据大小为: %llu", m_v2ReportStatData.size());

		// 将源数据中的空值字符串("NULL")转换为("0")
		base::PubStr::ReplaceInStrVector2(m_v2ReportStatData, "NULL", "0", false, true);
	}
	else
	{
		// 将源数据中的空值字符串("NULL")转换为("0")
		const int VEC3_SIZE = m_v3HiveSrcData.size();
		for ( int i = 0; i < VEC3_SIZE; ++i )
		{
			std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

			base::PubStr::ReplaceInStrVector2(ref_vec2, "NULL", "0", false, true);
		}
	}

	m_pLog->Output("[Analyse] 分析完成!");
}

void Analyse::TransSrcDataToReportStatData()
{
	std::map<std::string, std::vector<std::string> > 	mReportStatData;
	std::map<std::string, std::vector<std::string> >::iterator	it = mReportStatData.end();

	std::string str_tmp;
	std::string m_key;
	bool transfer_ok = true;
	size_t ignore_count = 0;

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

		const size_t VEC2_SIZE = ref_vec2.size();
		for ( size_t j = 0; j < VEC2_SIZE; ++j )
		{
			std::vector<std::string>& ref_vec1 = ref_vec2[j];

			// 默认：统一编码转换成功
			transfer_ok = true;

			// 组织key值
			m_key.clear();
			const int VEC1_SIZE = ref_vec1.size();
			const int DIM_SIZE = VEC1_SIZE - 1;
			for ( int k = 0; k < DIM_SIZE; ++k )
			{
				std::string& ref_str = ref_vec1[k];

				// 统一编码转换
				if ( !m_UniCodeTransfer.Transfer(ref_str, ref_str) )
				{
					transfer_ok = false;
					break;
				}

				m_key += ref_str;
			}

			// 统一编码转换失败，则忽略该数据
			if ( !transfer_ok )
			{
				++ignore_count;		// 统计忽略的数据条数
				continue;
			}

			// 值所在的列序号
			const int VAL_INDEX = DIM_SIZE + i;

			it = mReportStatData.find(m_key);
			if ( it != mReportStatData.end() )		// key值存在
			{
				std::vector<std::string>& ref_m_v = it->second;

				ref_m_v[VAL_INDEX] = ref_vec1[DIM_SIZE];
			}
			else		// key值不存在
			{
				str_tmp = ref_vec1[DIM_SIZE];
				//ref_vec1[DIM_SIZE] = "NULL";
				ref_vec1[DIM_SIZE] = "";		// 置为空

				const int TOTAL_SIZE = VEC3_SIZE + VEC1_SIZE - 1;
				for ( int l = VEC1_SIZE; l < TOTAL_SIZE; ++l )
				{
					//ref_vec1.push_back("NULL");
					ref_vec1.push_back("");		// 置为空
				}
				ref_vec1[VAL_INDEX] = str_tmp;

				mReportStatData[m_key] = ref_vec1;
			}
		}
	}

	m_pLog->Output("[Analyse] 因统一编码转换失败，而被忽略的数据条数：%llu", ignore_count);

	std::vector<std::vector<std::string> > vec2_reportdata;
	for ( it = mReportStatData.begin(); it != mReportStatData.end(); ++it )
	{
		base::PubStr::VVectorSwapPushBack(vec2_reportdata, it->second);
	}
	vec2_reportdata.swap(m_v2ReportStatData);

	// 释放Hive源数据
	std::vector<std::vector<std::vector<std::string> > >().swap(m_v3HiveSrcData);
}

void Analyse::CollectDimVal(AnaTaskInfo& info)
{
	m_pLog->Output("[Analyse] 收集源数据的所有维度取值 ...");

	DimVal dv;
	dv.KpiID = info.KpiID;

	std::vector<KpiColumn>& v_dimcol = info.vecKpiDimCol;
	int vec_size = v_dimcol.size();

	// 收集有效维度 index
	std::vector<int> vec_dim_index;
	for ( int i = 0; i < vec_size; ++i )
	{
		// 特殊维度，不存在于源数据中
		if ( v_dimcol[i].ColSeq < 0 )
		{
			continue;
		}

		vec_dim_index.push_back(i);
	}
	vec_size = vec_dim_index.size();

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

		const size_t VEC2_SIZE = ref_vec2.size();
		for ( size_t j = 0; j < VEC2_SIZE; ++j )
		{
			std::vector<std::string>& ref_vec1 = ref_vec2[j];

			for ( int k = 0; k < vec_size; ++k )
			{
				int& index = vec_dim_index[k];
				KpiColumn& ref_col = v_dimcol[index];

				// 只收集列表显示类型
				if ( KpiColumn::DTYPE_LIST == ref_col.DisType )
				{
					dv.DBName = ref_col.DBName;
					dv.Value  = ref_vec1[k];

					m_DVDiffer.FetchSrcDimVal(dv);
				}
			}
		}
	}

	m_pLog->Output("[Analyse] 收集到源数据的维度取值数: %llu", m_DVDiffer.GetSrcDimValSize());
}

void Analyse::StoreResult(AnaTaskInfo& t_info) throw(base::Exception)
{
	// 删除旧的数据
	RemoveOldResult();

	// 是否为报表统计类型
	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == t_info.AnaRule.AnaType )	// 报表统计类型
	{
		m_pLog->Output("[Analyse] 准备入库报表统计结果数据 ...");
		m_pAnaDB2->InsertReportStatData(m_dbinfo, m_v2ReportStatData);
	}
	else		// 其他类型
	{
		const int VEC3_SIZE = m_v3HiveSrcData.size();
		for ( int i = 0; i < VEC3_SIZE; ++i )
		{
			m_pLog->Output("[Analyse] 准备入库第 %d 组结果数据 ...", i+1);
			m_pAnaDB2->InsertResultData(m_dbinfo, m_v3HiveSrcData[i]);
		}
	}

	m_pLog->Output("[Analyse] 结果数据存储完毕!");
}

void Analyse::RemoveOldResult() throw(base::Exception)
{
	// 是否带时间戳
	// 只有带时间戳才可以按采集时间删除结果数据
	if ( m_dbinfo.time_stamp )
	{
		m_pLog->Output("[Analyse] 统计已存在的旧的结果数据 ...");
		const size_t NUM_OF_REPORT_DATA = m_pAnaDB2->SelectResultData(m_dbinfo);
		m_pLog->Output("[Analyse] 统计到的旧结果数据大小: %llu ( DATE_TIME: %s )", NUM_OF_REPORT_DATA, m_dbinfo.date_time.c_str());

		// 是否存在旧的结果数据
		if ( NUM_OF_REPORT_DATA > 0 )
		{
			m_pLog->Output("[Analyse] 删除已存在的旧的结果数据 ...");
			m_pAnaDB2->DeleteResultData(m_dbinfo);
		}
	}
	else
	{
		m_pLog->Output("[Analyse] 没有需要删除的旧结果数据.");
	}
}

void Analyse::AlarmJudgement(AnaTaskInfo& info) throw(base::Exception)
{
	// 是否有配置告警？
	if ( info.vecAlarm.empty() )
	{
		m_pLog->Output("[Analyse] 无告警配置：不产生告警!");
		return;
	}

	const int VEC_SIZE = info.vecAlarm.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		AlarmRule& ref_rule = info.vecAlarm[i];
		m_pLog->Output("[Analyse] 告警判断：告警 %d <ID:%s, NAME:%d>", (i+1), ref_rule.AlarmID.c_str(), ref_rule.AlarmName.c_str());

		if ( ref_rule.AlarmExpress.empty() )
		{
			throw base::Exception(ANAERR_ALARM_JUDGEMENT_FAILED, "告警规则表达式缺失！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), ref_rule.AlarmID.c_str(), __FILE__, __LINE__);
		}

		switch ( ref_rule.AlarmType )
		{
		case AlarmRule::AT_FLUCTUATE:
			m_pLog->Output("[Analyse] 告警类型：波动告警");
			FluctuateAlarm(info, ref_rule);
			break;
		case AlarmRule::AT_RATIO:
			m_pLog->Output("[Analyse] 告警类型：对比告警");
			RatioAlarm(info, ref_rule);
			break;
		case AlarmRule::AT_UNKNOWN:
		default:
			throw base::Exception(ANAERR_ALARM_JUDGEMENT_FAILED, "无法识别的告警规则类型：AT_UNKNOWN！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), ref_rule.AlarmID.c_str(), __FILE__, __LINE__);
			break;
		}
	}
}

void Analyse::FluctuateAlarm(AnaTaskInfo& info, AlarmRule& alarm_rule) throw(base::Exception)
{
	AlarmFluctuate alarm_flu;
	alarm_flu.SetTaskDBInfo(info, m_dbinfo);
	alarm_flu.SetAlarmRule(alarm_rule);
	alarm_flu.AnalyseExpression();

	const std::string ALARM_DATE = alarm_flu.GetFluctuateDate();
	m_pLog->Output("[Analyse] 准备目标表(%s)的比对数据 (时间:%s) ...", m_dbinfo.target_table.c_str(), ALARM_DATE.c_str());
	std::vector<std::vector<std::string> > vec2_old_data;
	m_pAnaDB2->SelectTargetData(m_dbinfo, ALARM_DATE, vec2_old_data);
	m_pLog->Output("[Analyse] 获得比对数据，大小为：%llu", vec2_old_data.size());

	alarm_flu.SetCompareData(vec2_old_data);

	// 是否为报表数据
	if ( info.AnaRule.AnaType != AnalyseRule::ANATYPE_REPORT_STATISTICS )	// 非报表数据
	{
		const int VEC3_SIZE = m_v3HiveSrcData.size();
		for ( int i = 0; i < VEC3_SIZE; ++i )
		{
			std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

			alarm_flu.AnalyseTargetData(ref_vec2);
		}
	}
	else	// 报表数据
	{
		alarm_flu.AnalyseTargetData(m_v2ReportStatData);
	}

	// 生成告警事件
	std::vector<AlarmEvent> vec_event;
	if ( alarm_flu.GenerateAlarmEvent(vec_event) )		// 产生告警事件
	{
		HandleAlarmEvent(vec_event);
	}
	else		// 无告警事件
	{
		m_pLog->Output("[Analyse] 无告警生成！(ALARM_ID:%s, ALARM_NAME:%s)", alarm_rule.AlarmID.c_str(), alarm_rule.AlarmName.c_str());
	}
}

void Analyse::RatioAlarm(AnaTaskInfo& info, AlarmRule& alarm_rule) throw(base::Exception)
{
	m_pLog->Output("[Analyse] 对比告警暂不支持！");
	return;

	AlarmRatio alarm_rat;
	alarm_rat.SetTaskDBInfo(info, m_dbinfo);
	alarm_rat.SetAlarmRule(alarm_rule);
	alarm_rat.AnalyseExpression();

	// 是否为报表数据
	if ( info.AnaRule.AnaType != AnalyseRule::ANATYPE_REPORT_STATISTICS )	// 非报表数据
	{
		const int VEC3_SIZE = m_v3HiveSrcData.size();
		for ( int i = 0; i < VEC3_SIZE; ++i )
		{
			std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

			alarm_rat.AnalyseTargetData(ref_vec2);
		}
	}
	else	// 报表数据
	{
		alarm_rat.AnalyseTargetData(m_v2ReportStatData);
	}

	// 生成告警事件
	std::vector<AlarmEvent> vec_event;
	if ( alarm_rat.GenerateAlarmEvent(vec_event) )
	{
		HandleAlarmEvent(vec_event);
	}
	else
	{
		m_pLog->Output("[Analyse] 无告警生成！(ALARM_ID:%s, ALARM_NAME:%s)", alarm_rule.AlarmID.c_str(), alarm_rule.AlarmName.c_str());
	}
}

void Analyse::HandleAlarmEvent(std::vector<AlarmEvent>& vec_event) throw(base::Exception)
{
	if ( vec_event.empty() )
	{
		m_pLog->Output("[Analyse] 无告警事件需要处理！");
	}
	else
	{
		m_pLog->Output("[Analyse] 处理告警事件 ...");

		int event_id = 0;
		if ( m_pAnaDB2->SelectMaxAlarmEventID(event_id) )
		{
			m_pLog->Output("[Analyse] 告警事件表 (%s) 中最大告警事件ID: %d", m_tabAlarmEvent.c_str(), event_id);

			++event_id;		// 最大告警事件 ID + 1
		}
		else
		{
			m_pLog->Output("[Analyse] 告警事件表 (%s) 中无告警事件记录！", m_tabAlarmEvent.c_str());

			event_id = 1;
			m_pLog->Output("[Analyse] 默认告警事件 ID 从 1 开始.");
		}

		// 设置告警事件 ID
		m_pLog->Output("[Analyse] 设置告警事件 ID ...");
		const int VEC_EVENT_SIZE = vec_event.size();
		for ( int i = 0; i < VEC_EVENT_SIZE; ++i )
		{
			AlarmEvent& ref_event = vec_event[i];
			ref_event.eventID = event_id++;
		}

		// 告警事件入库
		m_pLog->Output("[Analyse] 登记告警事件 ...");
		m_pAnaDB2->InsertAlarmEvent(vec_event);

		m_pLog->Output("[Analyse] 成功登记告警事件：%lu", VEC_EVENT_SIZE);
	}
}

