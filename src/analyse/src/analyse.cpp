#include "analyse.h"
#include <vector>
#include <set>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "log.h"
#include "simpletime.h"
#include "autodisconnect.h"
#include "pubstr.h"
#include "canadb2.h"
#include "chivethrift.h"
#include "anadbinfo.h"
#include "taskinfoutil.h"


Analyse g_Analyse;


Analyse::Analyse()
:m_nHivePort(0)
,m_pAnaDB2(NULL)
,m_pCHive(NULL)
{
	g_pApp = &g_Analyse;
}

Analyse::~Analyse()
{
}

const char* Analyse::Version()
{
	return ("Analyse: Version 1.02.0040 released. Compiled at "__TIME__" on "__DATE__);
}

void Analyse::LoadConfig() throw(base::Exception)
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");
	m_cfg.RegisterItem("HIVE_SERVER", "IP_ADDRESS");
	m_cfg.RegisterItem("HIVE_SERVER", "PORT");

	m_cfg.RegisterItem("TABLE", "TAB_KPI_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_KPI_COLUMN");
	m_cfg.RegisterItem("TABLE", "TAB_DIM_VALUE");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_DIM");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_VAL");
	m_cfg.RegisterItem("TABLE", "TAB_ANA_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_EVENT");

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
		throw base::Exception(ANAERR_HIVE_PORT_INVALID, "Hive服务器端口无效! (port=%d) [FILE:%s, LINE:%d]", m_nHivePort, __FILE__, __LINE__);
	}

	// Tables
	m_tabKpiRule   = m_cfg.GetCfgValue("TABLE", "TAB_KPI_RULE");
	m_tabKpiColumn = m_cfg.GetCfgValue("TABLE", "TAB_KPI_COLUMN");
	m_tabDimValue  = m_cfg.GetCfgValue("TABLE", "TAB_DIM_VALUE");
	m_tabEtlRule   = m_cfg.GetCfgValue("TABLE", "TAB_ETL_RULE");
	m_tabEtlDim    = m_cfg.GetCfgValue("TABLE", "TAB_ETL_DIM");
	m_tabEtlVal    = m_cfg.GetCfgValue("TABLE", "TAB_ETL_VAL");
	m_tabAnaRule   = m_cfg.GetCfgValue("TABLE", "TAB_ANA_RULE");
	m_tabAlarmRule = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_RULE");
	m_tabAlarmEvent = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_EVENT");

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

	m_pHiveThrift = new CHiveThrift(m_sHiveIP, m_nHivePort);
	if ( NULL == m_pHiveThrift )
	{
		throw base::Exception(ANAERR_INIT_FAILED, "new CHiveThrift failed: 无法申请到内存空间!");
	}
	m_pCHive = dynamic_cast<CHiveThrift*>(m_pHiveThrift);

	m_pCHive->Init();

	m_pLog->Output("Init OK.");
}

void Analyse::Run() throw(base::Exception)
{
	base::AutoDisconnect a_disconn(m_pAnaDB2, m_pCHive);
	a_disconn.Connect();

	AnaTaskInfo task_info;
	SetTaskInfo(task_info);

	FetchTaskInfo(task_info);

	DoDataAnalyse(task_info);
}

void Analyse::GetParameterTaskInfo(const std::string& para) throw(base::Exception)
{
	// 格式：启动批号|指标ID|分析规则ID|...
	std::vector<std::string> vec_str;
	boost::split(vec_str, para, boost::is_any_of("|"));

	if ( vec_str.size() < 3 )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法按格式拆分! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	m_sKpiID = vec_str[1];
	boost::trim(m_sKpiID);
	if ( m_sKpiID.empty() )
	{
		throw base::Exception(ANAERR_KPIID_INVALID, "指标ID无效! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_sAnaID = vec_str[2];
	boost::trim(m_sAnaID);
	if ( m_sAnaID.empty() )
	{
		throw base::Exception(ANAERR_ANAID_INVALID, "分析规则ID无效! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[Analyse] 任务参数信息：指标ID [KPI_ID:%s], 分析规则ID [ANA_ID:%s]", m_sKpiID.c_str(), m_sAnaID.c_str());
}

void Analyse::GetAnaDBInfo(AnaTaskInfo& t_info, AnaDBInfo& db_info) throw(base::Exception)
{
	db_info.time_stamp = false;
	std::string str_tmp;

	std::vector<std::string> v_fields;
	int v_size = t_info.vecKpiDimCol.size();
	for ( int i = 0; i < v_size; ++i )
	{
		KpiColumn& col = t_info.vecKpiDimCol[i];

		if ( col.DBName.empty() )
		{
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "维度字段名为空! 无效! (KPI_ID=%s, COL_TYPE=%s, COL_SEQ=%d) [FILE:%s, LINE:%d]", col.KpiID.c_str(), col.ColType.c_str(), col.ColSeq, __FILE__, __LINE__);
		}

		if ( col.ColSeq < 0 )
		{
			if ( -1 == col.ColSeq )		// ColType为CTYPE_DIM时, col.ColSeq = -1 表示时间戳
			{
				if ( !db_info.time_stamp )
				{
					db_info.time_stamp = true;
					str_tmp = col.DBName;
				}
				else
				{
					throw base::Exception(ANAERR_GET_DBINFO_FAILED, "时间戳维度字段重复设置: KPI_ID=%s, COL_TYPE=%s, COL_SEQ=%d, DB_NAME=%s [FILE:%s, LINE:%d]", col.KpiID.c_str(), col.ColType.c_str(), col.ColSeq, col.DBName.c_str(), __FILE__, __LINE__);
				}
			}
			else
			{
				throw base::Exception(ANAERR_GET_DBINFO_FAILED, "无法识别的维度字段序号: %d (KPI_ID=%s, COL_TYPE=%s, DB_NAME=%s) [FILE:%s, LINE:%d]", col.ColSeq, col.KpiID.c_str(), col.ColType.c_str(), col.DBName.c_str(), __FILE__, __LINE__);
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
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "值字段名为空! 无效! (KPI_ID=%s, COL_TYPE=%s, COL_SEQ=%d) [FILE:%s, LINE:%d]", col.KpiID.c_str(), col.ColType.c_str(), col.ColSeq, __FILE__, __LINE__);
		}

		if ( col.ColSeq < 0 )
		{
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "无法识别的值字段序号: %d (KPI_ID=%s, COL_TYPE=%s, DB_NAME=%s) [FILE:%s, LINE:%d]", col.ColSeq, col.KpiID.c_str(), col.ColType.c_str(), col.DBName.c_str(), __FILE__, __LINE__);
		}

		v_fields.push_back(col.DBName);
	}

	// 加时间戳
	if ( db_info.time_stamp )
	{
		v_fields.push_back(str_tmp);
	}

	// 按表的类型生成最终目标表名
	db_info.target_table = GenerateTableNameByType(t_info);

	// 组织数据库入库SQL语句
	db_info.db2_sql = "insert into " + db_info.target_table + "(";

	str_tmp.clear();
	v_size = v_fields.size();
	for ( int i = 0; i < v_size; ++i )
	{
		if ( i != 0 )
		{
			db_info.db2_sql += ", " + v_fields[i];

			str_tmp += ", ?";
		}
		else
		{
			db_info.db2_sql += v_fields[i];

			str_tmp += "?";
		}
	}

	db_info.db2_sql += ") values(" + str_tmp + ")";

	v_fields.swap(db_info.vec_fields);
}

void Analyse::FetchHiveSource(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	std::vector<std::vector<std::string> > vec2_fields;

	const int SQL_SIZE = vec_hivesql.size();
	for ( int i = 0; i < SQL_SIZE; ++i )
	{
		m_pCHive->FetchSourceData(vec_hivesql[i], vec2_fields);

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

	m_pAnaDB2->InsertNewDimValue(vec_diff_dv);
	m_pLog->Output("[Analyse] 更新维度取值范围成功! Update size: %lu", vec_diff_dv.size());
}

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

	if ( info.vecKpiDimCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标维度信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( info.vecKpiValCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标值信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}
}

void Analyse::DoDataAnalyse(AnaTaskInfo& t_info) throw(base::Exception)
{
	m_pLog->Output("[Analyse] 解析分析规则 ...");

	std::vector<std::string> vec_hivesql;
	AnaDBInfo db_info;
	AnalyseRules(t_info, vec_hivesql, db_info);

	m_pLog->Output("[Analyse] 获取Hive源数据 ...");
	FetchHiveSource(vec_hivesql);

	m_pLog->Output("[Analyse] 分析源数据 ...");
	AnalyseSourceData(t_info, db_info);

	m_pLog->Output("[Analyse] 生成结果数据 ...");
	StoreResult(t_info, db_info);

	m_pLog->Output("[Analyse] 告警判断 ...");
	AlarmJudgement(t_info);

	m_pLog->Output("[Analyse] 更新维度取值范围 ...");
	UpdateDimValue(t_info);
}

void Analyse::AnalyseRules(AnaTaskInfo& t_info, std::vector<std::string>& vec_hivesql, size_t& fields_num, AnaDBInfo& db_info) throw(base::Exception)
{
	// 分析规则类型
	switch ( t_info.AnaRule.AnaType )
	{
	case AnalyseRule::ANATYPE_SUMMARY_COMPARE:		// 汇总对比
		m_pLog->Output("[Analyse] 分析规则类型：汇总对比 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", t_info.AnaRule.AnaExpress.c_str());
		GetSummaryCompareHiveSQL(t_info, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_DETAIL_COMPARE:		// 明细对比
		m_pLog->Output("[Analyse] 分析规则类型：明细对比 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", t_info.AnaRule.AnaExpress.c_str());
		GetDetailCompareHiveSQL(t_info, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_STATISTICS:			// 一般统计
		m_pLog->Output("[Analyse] 分析规则类型：一般统计 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		//m_pLog->Output("[Analyse] 分析规则表达式：%s", t_info.AnaRule.AnaExpress.c_str());
		GetStatisticsHiveSQL(t_info, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_REPORT_STATISTICS:	// 报表统计
		m_pLog->Output("[Analyse] 分析规则类型：报表统计 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		//m_pLog->Output("[Analyse] 分析规则表达式：%s", t_info.AnaRule.AnaExpress.c_str());
		GetReportStatisticsHiveSQL(t_info, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_HIVE_SQL:				// 可执行的HIVE SQL语句
		m_pLog->Output("[Analyse] 分析规则类型：可执行的HIVE SQL语句 (KPI_ID:%s, ANA_ID:%s)", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str());
		// 分析表达式，即是可执行的Hive SQL语句（可多个，以分号分隔）
		SplitHiveSqlExpress(t_info.AnaRule.AnaExpress, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_UNKNOWN:				// 未知类型
	default:
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "无法识别的分析规则类型: ANATYPE_UNKNOWN (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	// 生成数据库[DB2]信息
	GetAnaDBInfo(t_info, db_info);
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

	std::set<int> set_diff;			// 用于查重
	std::vector<int> vec_col;
	const int DIFF_SIZE = vec_str.size();
	for ( int i = 0; i < DIFF_SIZE; ++i )
	{
		std::string& ref_str = vec_str[i];

		boost::to_upper(ref_str);
		size_t pos = ref_str.find("DIFF");
		if ( std::string::npos == pos )
		{
			throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}

		int diff_no   = -1;
		try
		{
			diff_no = boost::lexical_cast<int>(ref_str.substr(pos+4));
		}
		catch ( boost::bad_lexical_cast& ex )
		{
			throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：[%s] 转换失败! [BOOST] %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_str.c_str(), ex.what(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}

		if ( diff_no < 1 || diff_no > val_size_1 )
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

	// 1) 汇总：对平的Hive SQL语句
	std::string hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col);
	hive_sql += ", '对平' from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += " and " + TaskInfoUtil::GetCompareEqualValsByCol(first_one, second_one, vec_col) + ")";

	v_hive_sql.push_back(hive_sql);

	// 2) 汇总：有差异的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col);
	hive_sql += ", '有差异' from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetCompareUnequalValsByCol(first_one, second_one, vec_col);

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

	// 1) 明细：对平的Hive SQL语句
	std::string hive_sql = "select " + TaskInfoUtil::GetCompareFields(first_one, second_one);
	hive_sql += ", '对平' from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += " and " + TaskInfoUtil::GetCompareEqualVals(first_one, second_one) + ")";

	v_hive_sql.push_back(hive_sql);

	// 2) 明细：有差异的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFields(first_one, second_one);
	hive_sql += ", '有差异' from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetCompareUnequalVals(first_one, second_one);

	v_hive_sql.push_back(hive_sql);

	// 3) 明细：左有右无的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFields(first_one, second_one);
	hive_sql += ", '左有右无' from " + first_one.TargetPatch + " a left outer join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetOneRuleValsNull(second_one, "b.");

	v_hive_sql.push_back(hive_sql);

	// 4) 明细：左无右有的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFields(second_one, first_one, true);
	hive_sql += ", '左无右有' from " + second_one.TargetPatch + " b left outer join " + first_one.TargetPatch;
	hive_sql += " a on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetOneRuleValsNull(first_one, "a.");

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

	// 暂不使用分析规则表达式
	//std::string& ana_exp = t_info.AnaRule.AnaExpress;

	TaskInfoUtil(t_info.vecEtlRule, vec_hivesql);
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

	// 暂不使用分析规则表达式
	//std::string& ana_exp = t_info.AnaRule.AnaExpress;

	TaskInfoUtil(t_info.vecEtlRule, vec_hivesql);
}

void Analyse::SplitHiveSqlExpress(std::string exp, std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	boost::trim(exp);
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
	boost::to_upper(first_group);
	boost::to_upper(second_group);

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

std::string Analyse::GenerateTableNameByType(AnaTaskInfo& info) throw(base::Exception)
{
	AnaTaskInfo::ResultTableType& type = info.ResultType;

	std::string tab_name = info.TableName;
	boost::trim(tab_name);

	switch ( type )
	{
	case AnaTaskInfo::TABTYPE_COMMON:		// 普通表
		// Do nothing
		break;
	case AnaTaskInfo::TABTYPE_DAY:			// 天表
		tab_name = tab_name + "_" + base::SimpleTime::Now().DayTime8();
		break;
	case AnaTaskInfo::TABTYPE_MONTH:		// 月表
		tab_name = tab_name + "_" + base::SimpleTime::Now().MonTime6();
		break;
	case AnaTaskInfo::TABTYPE_YEAR:			// 年表
		tab_name = tab_name + "_" + base::SimpleTime::Now().YearTime();
		break;
	case AnaTaskInfo::TABTYPE_UNKNOWN:		// 未知类型
	default:
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "无法识别的目标表类型: TABTYPE_UNKNOWN (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Analyse] 最终目标表名：%s", tab_name.c_str());
	return tab_name;
}

void Analyse::AnalyseSourceData(AnaTaskInfo& t_info, AnaDBInfo& db_info) throw(base::Exception)
{
	// 报表统计
	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == t_info.AnaRule.AnaType )
	{
		m_pLog->Output("[Analyse] 报表统计类型数据转换 ...");

		m_pLog->Output("[Analyse] 转换前, 数据大小为: %llu", base::PubStr::CalcVVVectorStr(m_v3HiveSrcData));
		TransSrcDataToReportStatData();
		m_pLog->Output("[Analyse] 转换后, 数据大小为: %llu", m_v2ReportStatData.size());

		std::string now_day = base::SimpleTime::Now().DayTime8();
		m_pLog->Output("[Analyse] 删除已存在的旧报表统计数据, 时间为: %s", now_day.c_str());
		m_pAnaDB2->DeleteReportStatData(db_info, now_day);
	}

	m_pLog->Output("[Analyse] 分析完成!");
}

void Analyse::TransSrcDataToReportStatData()
{
	// 释放Hive源数据
	m_v3HiveSrcData.clear();
}

void Analyse::CollectDimVal(AnaTaskInfo& info)
{
	m_pLog->Output("[Analyse] 收集源数据的所有维度取值 ...");

	DimVal dv;
	dv.KpiID = info.KpiID;

	std::vector<KpiColumn>& v_dimcol = info.vecKpiDimCol;
	const int DIM_SIZE = v_dimcol.size();

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

		const size_t VEC2_SIZE = ref_vec2.size();
		for ( size_t j = 0; j < VEC2_SIZE; ++j )
		{
			std::vector<std::string>& ref_vec1 = ref_vec2[j];

			for ( int k = 0; k < DIM_SIZE; ++k )
			{
				dv.DBName = v_dimcol[k].DBName;
				dv.Value  = ref_vec1[k];

				m_DVDiffer.FetchSrcDimVal(dv);
			}
		}
	}

	m_pLog->Output("[Analyse] 收集到源数据的维度取值数: %llu", m_DVDiffer.GetSrcDimValSize());
}

void Analyse::StoreResult(AnaTaskInfo& t_info, AnaDBInfo& db_info) throw(base::Exception)
{
	const int VEC3_SIZE = m_v3HiveSrcData.size();

	// 是否为报表统计类型
	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == t_info.AnaRule.AnaType )	// 报表统计类型
	{
		m_pAnaDB2->InsertReportStatData(db_info, m_v2ReportStatData);
	}
	else		// 其他类型
	{
		for ( int i = 0; i < VEC3_SIZE; ++i )
		{
			m_pAnaDB2->InsertResultData(db_info, m_v3HiveSrcData[i]);
		}
	}

	m_pLog->Output("[Analyse] 结果数据存储完毕!");
}

void Analyse::AlarmJudgement(AnaTaskInfo& info)
{
	m_pLog->Output("[Analyse] 无告警产生!");
}

