#include "analyse.h"
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "log.h"
#include "simpletime.h"
#include "autodisconnect.h"
#include "canadb2.h"
#include "chivethrift.h"
#include "anadbinfo.h"


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
	return ("Analyse: Version 1.00.0035 released. Compiled at "__TIME__" on "__DATE__);
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
		throw base::Exception(ANAERR_TASKINFO_ERROR, "参数任务信息异常(split size:%lu), 无法按格式拆分! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
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
}

size_t Analyse::GetTotalNumOfTargetFields(AnaTaskInfo& info)
{
	// 无效维度字段计数 
	// 其中，无效是指该维度字段不从源数据中获得
	size_t invalid_dim_count = 0;

	// 统计无效维度字段
	const size_t DIM_SIZE = info.vecKpiDimCol.size();
	for ( size_t i = 0; i < DIM_SIZE; ++i )
	{
		KpiColumn& col = info.vecKpiDimCol[i];

		if ( col.ColSeq < 0 )
		{
			++invalid_dim_count;
		}
	}

	return (DIM_SIZE + info.vecKpiValCol.size() - invalid_dim_count);
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
			if ( -1 == col.ColSeq )
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

void Analyse::FetchHiveSource(const std::string& hive_sql, const size_t& total_num_of_fields, std::vector<std::vector<std::string> >& vv_fields) throw(base::Exception)
{
	m_pCHive->FetchSourceData(hive_sql, total_num_of_fields, vv_fields);
	m_pLog->Output("[Analyse] 获取源数据记录数：%llu", vv_fields.size());
}

void Analyse::UpdateDimValue(const std::string& kpi_id)
{
	m_pAnaDB2->SelectDimValue(kpi_id, m_DVDiffer);
	m_pLog->Output("[Analyse] 从数据库中获取指标 (KPI_ID:%s) 的维度取值范围, size: %llu", kpi_id.c_str(), m_DVDiffer.GetDBDimValSize());

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
	if ( info.ResultType.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "分析结果表类型为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( info.TableName.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "分析结果表为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( info.AnaRule.AnaType.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "分析规则类型为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( info.AnaRule.AnaExpress.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "分析规则表达式为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

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

	std::string hive_sql;
	size_t total_num_of_fields = 0;
	AnaDBInfo db_info;
	AnalyseRules(t_info, hive_sql, total_num_of_fields, db_info);

	m_pLog->Output("[Analyse] 获取Hive源数据 ...");
	std::vector<std::vector<std::string> > vec_vec_fields;
	FetchHiveSource(hive_sql, total_num_of_fields, vec_vec_fields);

	m_pLog->Output("[Analyse] 分析源数据 ...");
	AnalyseSource(t_info, vec_vec_fields);

	m_pLog->Output("[Analyse] 生成结果数据 ...");
	StoreResult(db_info, vec_vec_fields);

	m_pLog->Output("[Analyse] 告警判断 ...");
	AlarmJudgement(t_info, vec_vec_fields);

	m_pLog->Output("[Analyse] 更新维度取值范围 ...");
	UpdateDimValue(t_info.KpiID);
}

void Analyse::AnalyseRules(AnaTaskInfo& t_info, std::string& hive_sql, size_t& fields_num, AnaDBInfo& db_info) throw(base::Exception)
{
	std::string& ana_type = t_info.AnaRule.AnaType;
	boost::trim(ana_type);
	boost::to_upper(ana_type);

	// 分析类型
	if ( "SUMMARY" == ana_type )	// 汇总对比
	{
		hive_sql = GetSummaryCompareHiveSQL(t_info);
	}
	else if ( "DETAIL" == ana_type )	// 明细对比
	{
		hive_sql = GetDetailCompareHiveSQL(t_info);
	}
	else if ( "HIVE_SQL" == ana_type )		// 可执行的Hive SQL语句
	{
		// 分析表达式，即是可执行的Hive SQL语句
		hive_sql = t_info.AnaRule.AnaExpress;
	}
	else
	{
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "无法识别的分析类型: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_type.c_str(), t_info.KpiID.c_str(), t_info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	// 统计从源数据插入到目标表的字段总数
	fields_num = GetTotalNumOfTargetFields(t_info);

	// 生成数据库[DB2]信息
	GetAnaDBInfo(t_info, db_info);
}

std::string Analyse::GetSummaryCompareHiveSQL(AnaTaskInfo& t_info)
{
	return std::string();
}

std::string Analyse::GetDetailCompareHiveSQL(AnaTaskInfo& t_info)
{
	return std::string();
}

std::string Analyse::GenerateTableNameByType(AnaTaskInfo& info) throw(base::Exception)
{
	std::string& type = info.ResultType;
	boost::trim(type);
	boost::to_upper(type);

	std::string tab_name = info.TableName;
	boost::trim(tab_name);

	if ( "SINGLE_TABLE" == type )		// 与最终目标表名一致
	{
		// Do nothing
	}
	else if ( "DAY_TABLE" == type )		// 天表
	{
		tab_name = tab_name + "_" + base::SimpleTime::Now().DayTime8();
	}
	else if ( "MONTH_TABLE" == type )	// 月表
	{
		tab_name = tab_name + "_" + base::SimpleTime::Now().MonTime6();
	}
	else if ( "YEAR_TABLE" == type )	// 年表
	{
		tab_name = tab_name + "_" + base::SimpleTime::Now().YearTime();
	}
	else
	{
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "无法识别的目标表类型: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", type.c_str(), info.KpiID.c_str(), info.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Analyse] 最终目标表名：%s", tab_name.c_str());
	return tab_name;
}

void Analyse::AnalyseSource(AnaTaskInfo& info, std::vector<std::vector<std::string> >& vec2_fields)
{
	CollectDimVal(info, vec2_fields);

	m_pLog->Output("[Analyse] 分析完成!");
}

void Analyse::CollectDimVal(AnaTaskInfo& info, std::vector<std::vector<std::string> >& vec2_fields)
{
	m_pLog->Output("[Analyse] 收集源数据的所有维度取值 ...");

	DimVal dv;
	dv.KpiID = info.KpiID;

	std::vector<KpiColumn>& v_dimcol = info.vecKpiDimCol;
	const int DIM_SIZE = v_dimcol.size();

	const size_t VEC2_SIZE = vec2_fields.size();
	for ( size_t i = 0; i < VEC2_SIZE; ++i )
	{
		std::vector<std::string>& v_field = vec2_fields[i];

		for ( int j = 0; j < DIM_SIZE; ++j )
		{
			dv.DBName = v_dimcol[j].DBName;
			dv.Value  = v_field[j];

			m_DVDiffer.FetchSrcDimVal(dv);
		}
	}

	m_pLog->Output("[Analyse] 收集到源数据的维度取值数: %llu", m_DVDiffer.GetSrcDimValSize());
}

void Analyse::StoreResult(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_fields) throw(base::Exception)
{
	m_pAnaDB2->InsertResultData(db_info, vec2_fields);
	m_pLog->Output("[Analyse] 结果数据存储完毕!");
}

void Analyse::AlarmJudgement(AnaTaskInfo& info, std::vector<std::vector<std::string> >& vec2_fields)
{
	m_pLog->Output("[Analyse] 无告警产生!");
}

