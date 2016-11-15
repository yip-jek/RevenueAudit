#include "analyse.h"
#include <vector>
#include <set>
#include <map>
#include "log.h"
#include "autodisconnect.h"
#include "pubstr.h"
#include "pubtime.h"
#include "simpletime.h"
#include "canadb2.h"
#include "canahive.h"
#include "taskinfoutil.h"
#include "alarmevent.h"
#include "alarmfluctuate.h"
#include "alarmratio.h"
//#include "compareresult.h"


Analyse g_Analyse;


Analyse::Analyse()
:m_pAnaDB2(NULL)
,m_pAnaHive(NULL)
#ifdef _YCRA_TASK
,m_ycSeqID(0)
#endif
{
	g_pApp = &g_Analyse;
}

Analyse::~Analyse()
{
}

const char* Analyse::Version()
{
	return ("Analyse: Version 2.0006.20161115 released. Compiled at "__TIME__" on "__DATE__);
}

void Analyse::LoadConfig() throw(base::Exception)
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");
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

#ifdef _YCRA_TASK
	m_cfg.RegisterItem("TABLE", "TAB_YC_TASK_REQ");
#endif

	m_cfg.ReadConfig();

	// 数据库配置
	m_sDBName  = m_cfg.GetCfgValue("DATABASE", "DB_NAME");
	m_sUsrName = m_cfg.GetCfgValue("DATABASE", "USER_NAME");
	m_sPasswd  = m_cfg.GetCfgValue("DATABASE", "PASSWORD");

	// Hive服务器配置
	m_zk_quorum    = m_cfg.GetCfgValue("HIVE_SERVER", "ZK_QUORUM");
	m_krb5_conf    = m_cfg.GetCfgValue("HIVE_SERVER", "KRB5_CONF");
	m_usr_keytab   = m_cfg.GetCfgValue("HIVE_SERVER", "USR_KEYTAB");
	m_principal    = m_cfg.GetCfgValue("HIVE_SERVER", "PRINCIPAL");
	m_jaas_conf    = m_cfg.GetCfgValue("HIVE_SERVER", "JAAS_CONF");
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

#ifdef _YCRA_TASK
	m_tabYCTaskReq   = m_cfg.GetCfgValue("TABLE", "TAB_YC_TASK_REQ");
#endif

	m_pLog->Output("[Analyse] Load configuration OK.");
}

std::string Analyse::GetLogFilePrefix()
{
	return std::string("Analyse");
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

	// 连接数据库
	m_pAnaDB2->Connect();

#ifdef _YCRA_TASK
	m_pAnaDB2->SetTabYCTaskReq(m_tabYCTaskReq);

	// 更新任务状态为；"21"（正在分析）
	m_pAnaDB2->UpdateYCTaskReq(m_ycSeqID, "21", "正在分析", "分析开始时间："+base::SimpleTime::Now().TimeStamp());
#endif

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

	m_pLog->Output("[Analyse] Init OK.");
}

void Analyse::Run() throw(base::Exception)
{
	base::AutoDisconnect a_disconn(new base::HiveConnector(m_pAnaHive));
	a_disconn.Connect();

	SetTaskInfo();

	FetchTaskInfo();

	DoDataAnalyse();
}

void Analyse::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
#ifdef _YCRA_TASK
	// 更新任务状态
	std::string task_desc;
	if ( 0 == err_code )	// 正常退出
	{
		// 更新任务状态为；"22"（分析完成）
		task_desc = "分析结束时间：" + base::SimpleTime::Now().TimeStamp();
		m_pAnaDB2->UpdateYCTaskReq(m_ycSeqID, "22", "分析完成", task_desc);
	}
	else	// 异常退出
	{
		// 更新任务状态为；"23"（分析失败）
		base::PubStr::SetFormatString(task_desc, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
		m_pAnaDB2->UpdateYCTaskReq(m_ycSeqID, "23", "分析失败", task_desc);
	}
#endif

	// 断开数据库连接
	if ( m_pAnaDB2 != NULL )
	{
		m_pAnaDB2->Disconnect();
	}
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

#ifdef _YCRA_TASK
	if ( vec_str.size() < 4 )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法拆分出业财任务流水号! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::T1TransT2(vec_str[3], m_ycSeqID) )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "无效的业财任务流水号：%s [FILE:%s, LINE:%d]", vec_str[3].c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Analyse] 业财稽核任务流水号：%d", m_ycSeqID);
#endif
}

void Analyse::GetAnaDBInfo() throw(base::Exception)
{
	m_dbinfo.day_now    = false;
	m_dbinfo.time_stamp = false;
	int index_dn = 0;
	int index_ts = 0;

	AnaField ana_field;
	std::vector<AnaField> v_fields;

	// 指标维度字段
	int v_size = m_taskInfo.vecKpiDimCol.size();
	for ( int i = 0; i < v_size; ++i )
	{
		KpiColumn& col = m_taskInfo.vecKpiDimCol[i];

		if ( col.DBName.empty() )
		{
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "维度字段名为空! 无效! (KPI_ID=%s, COL_TYPE=CTYPE_DIM, COL_SEQ=%d) [FILE:%s, LINE:%d]", col.KpiID.c_str(), col.ColSeq, __FILE__, __LINE__);
		}

		if ( col.ColSeq < 0 )
		{
			if ( -1 == col.ColSeq )		// ColType为CTYPE_DIM时, col.ColSeq=-1 表示时间戳
			{
				if ( !m_dbinfo.time_stamp )
				{
					m_dbinfo.time_stamp = true;
					index_ts = i;
				}
				else
				{
					throw base::Exception(ANAERR_GET_DBINFO_FAILED, "时间戳维度字段重复设置: KPI_ID=%s, COL_TYPE=CTYPE_DIM, COL_SEQ=%d, DB_NAME=%s [FILE:%s, LINE:%d]", col.KpiID.c_str(), col.ColSeq, col.DBName.c_str(), __FILE__, __LINE__);
				}
			}
			else if ( -2 == col.ColSeq )	// ColType为CTYPE_DIM时, col.ColSeq=-2 表示当前时间（天）
			{
				if ( !m_dbinfo.day_now )
				{
					m_dbinfo.day_now = true;
					index_dn = i;
				}
				else
				{
					throw base::Exception(ANAERR_GET_DBINFO_FAILED, "当前时间（天）维度字段重复设置: KPI_ID=%s, COL_TYPE=CTYPE_DIM, COL_SEQ=%d, DB_NAME=%s [FILE:%s, LINE:%d]", col.KpiID.c_str(), col.ColSeq, col.DBName.c_str(), __FILE__, __LINE__);
				}
			}
			else
			{
				throw base::Exception(ANAERR_GET_DBINFO_FAILED, "无法识别的维度字段序号: %d (KPI_ID=%s, COL_TYPE=CTYPE_DIM, DB_NAME=%s) [FILE:%s, LINE:%d]", col.ColSeq, col.KpiID.c_str(), col.DBName.c_str(), __FILE__, __LINE__);
			}
		}
		else
		{
			ana_field.field_name = col.DBName;
			ana_field.CN_name    = col.CNName;
			v_fields.push_back(ana_field);
		}
	}

	// 指标值字段
	std::string error_msg;
	if ( !TaskInfoUtil::KpiColumnPushBackAnaFields(v_fields, m_taskInfo.vecKpiValCol, error_msg) )
	{
		throw base::Exception(ANAERR_GET_DBINFO_FAILED, "指标值：%s [FILE:%s, LINE:%d]", error_msg.c_str(), __FILE__, __LINE__);
	}

	// 值的开始序号与维度的size相同
	m_dbinfo.val_beg_pos = m_taskInfo.vecKpiDimCol.size();

	// 指标单独显示的维度字段
	// 左侧
	if ( !TaskInfoUtil::KpiColumnPushBackAnaFields(v_fields, m_taskInfo.vecLeftKpiCol, error_msg) )
	{
		throw base::Exception(ANAERR_GET_DBINFO_FAILED, "左侧单独显示的指标维度：%s [FILE:%s, LINE:%d]", error_msg.c_str(), __FILE__, __LINE__);
	}
	// 右侧
	if ( !TaskInfoUtil::KpiColumnPushBackAnaFields(v_fields, m_taskInfo.vecRightKpiCol, error_msg) )
	{
		throw base::Exception(ANAERR_GET_DBINFO_FAILED, "右侧单独显示的指标维度：%s [FILE:%s, LINE:%d]", error_msg.c_str(), __FILE__, __LINE__);
	}

	// 加时间戳
	if ( m_dbinfo.time_stamp )
	{
		KpiColumn& ref_col = m_taskInfo.vecKpiDimCol[index_ts];
		ana_field.field_name = ref_col.DBName;
		ana_field.CN_name    = ref_col.CNName;
		v_fields.push_back(ana_field);

		// 时间戳维度在最后，值开始序号前移
		--(m_dbinfo.val_beg_pos);
	}

	// 加当前时间（天）
	if ( m_dbinfo.day_now )
	{
		KpiColumn& ref_col = m_taskInfo.vecKpiDimCol[index_dn];
		ana_field.field_name = ref_col.DBName;
		ana_field.CN_name    = ref_col.CNName;
		v_fields.push_back(ana_field);

		// 当前时间维度在最后，值开始序号前移
		--(m_dbinfo.val_beg_pos);
	}

	// 按表的类型生成最终目标表名
	GenerateTableNameByType();

	// 组织数据库入库SQL语句
	m_dbinfo.db2_sql = "insert into " + m_dbinfo.target_table + "(";

	std::string str_val_holder;
	v_size = v_fields.size();
	for ( int i = 0; i < v_size; ++i )
	{
		if ( i != 0 )
		{
			m_dbinfo.db2_sql += ", " + v_fields[i].field_name;

			str_val_holder += ", ?";
		}
		else
		{
			m_dbinfo.db2_sql += v_fields[i].field_name;

			str_val_holder += "?";
		}
	}

	m_dbinfo.db2_sql += ") values(" + str_val_holder + ")";

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

void Analyse::UpdateDimValue()
{
	// 业财稽核统计，无需更新维度取值范围！
	if ( AnalyseRule::ANATYPE_YC_STAT == m_taskInfo.AnaRule.AnaType )
	{
		return;
	}

	CollectDimVal();

	m_pAnaDB2->SelectDimValue(m_taskInfo.KpiID, m_DVDiffer);
	m_pLog->Output("[Analyse] 从数据库中获取指标 (KPI_ID:%s) 的维度取值范围, size: %llu", m_taskInfo.KpiID.c_str(), m_DVDiffer.GetDBDimValSize());

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

void Analyse::SetTaskInfo()
{
	m_taskInfo.KpiID         = m_sKpiID;
	m_taskInfo.AnaRule.AnaID = m_sAnaID;
}

void Analyse::FetchTaskInfo() throw(base::Exception)
{
	m_pLog->Output("[Analyse] 查询分析任务规则信息 ...");
	m_pAnaDB2->SelectAnaTaskInfo(m_taskInfo);

	m_pLog->Output("[Analyse] 检查分析任务规则信息 ...");
	CheckAnaTaskInfo();

	m_pLog->Output("[Analyse] 获取渠道、地市统一编码信息 ...");
	FetchUniformCode();

	GetYCRAStatRule();
}

void Analyse::CheckAnaTaskInfo() throw(base::Exception)
{
	if ( AnaTaskInfo::TABTYPE_UNKNOWN == m_taskInfo.ResultType )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "未知的分析结果表类型! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( m_taskInfo.TableName.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "分析结果表为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( AnalyseRule::ANATYPE_UNKNOWN == m_taskInfo.AnaRule.AnaType )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "未知的分析规则类型! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( AnalyseRule::ANATYPE_SUMMARY_COMPARE == m_taskInfo.AnaRule.AnaType )
	{
		// 汇总对比的结果描述共 2 个：对平，有差异
		if ( m_taskInfo.vecComResDesc.size() < (size_t)2 )
		{
			throw base::Exception(ANAERR_TASKINFO_INVALID, "汇总对比缺少结果描述，当前结果描述个数：%lu (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecComResDesc.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}
	}
	else if ( AnalyseRule::ANATYPE_DETAIL_COMPARE == m_taskInfo.AnaRule.AnaType )
	{
		// 明细对比的结果描述共 4 个：对平，有差异，左有右无，左无右有
		if ( m_taskInfo.vecComResDesc.size() < (size_t)4 )
		{
			throw base::Exception(ANAERR_TASKINFO_INVALID, "明细对比缺少结果描述，当前结果描述个数：%lu (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecComResDesc.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}
	}

	// 分析规则表达式可能为空！
	//if ( m_taskInfo.AnaRule.AnaExpress.empty() )
	//{
	//	throw base::Exception(ANAERR_TASKINFO_INVALID, "分析规则表达式为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	//}

	if ( m_taskInfo.vecEtlRule.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有采集规则信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( m_taskInfo.vecKpiDimCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标维度信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	if ( m_taskInfo.vecKpiValCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标值信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	//if ( AnalyseRule::ACTYPE_UNKNOWN == m_taskInfo.AnaRule.AnaCondType )
	//{
	//	throw base::Exception(ANAERR_TASKINFO_INVALID, "未知的分析条件类型! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	//}

	CheckExpWayType();
}

void Analyse::CheckExpWayType() throw(base::Exception)
{
	// 检查维度：是否存在重复的 '地市' 和 '渠道' 表示类型
	bool region_existed  = false;
	bool channel_existed = false;
	int vec_size = m_taskInfo.vecKpiDimCol.size();
	for ( int i = 0; i < vec_size; ++i )
	{
		KpiColumn& ref_kpi_dim = m_taskInfo.vecKpiDimCol[i];

		if ( KpiColumn::EWTYPE_REGION == ref_kpi_dim.ExpWay )	// 地市
		{
			if ( region_existed )	// 地市已经存在
			{
				throw base::Exception(ANAERR_TASKINFO_INVALID, "地市维度字段重复配置！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}
			else
			{
				region_existed = true;
			}
		}
		else if ( KpiColumn::EWTYPE_CHANNEL == ref_kpi_dim.ExpWay )	// 渠道
		{
			if ( channel_existed )	// 渠道已经存在
			{
				throw base::Exception(ANAERR_TASKINFO_INVALID, "渠道维度字段重复配置！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}
			else
			{
				channel_existed = true;
			}
		}
	}

	// 检查值：是否存在重复的 '对比结果' 表示类型
	bool comp_result_existed = false;
	vec_size = m_taskInfo.vecKpiValCol.size();
	for ( int j = 0; j < vec_size; ++j )
	{
		KpiColumn& ref_kpi_val = m_taskInfo.vecKpiValCol[j];

		if ( KpiColumn::EWTYPE_COMPARE_RESULT == ref_kpi_val.ExpWay )	// 对比结果
		{
			if ( comp_result_existed )	// 对比结果已经存在
			{
				throw base::Exception(ANAERR_TASKINFO_INVALID, "对比结果值字段重复配置！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}
			else
			{
				comp_result_existed = true;
			}
		}
	}
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

void Analyse::GetYCRAStatRule() throw(base::Exception)
{
	if ( AnalyseRule::ANATYPE_YC_STAT == m_taskInfo.AnaRule.AnaType )
	{
		m_pLog->Output("[Analyse] 获取业财稽核因子规则信息 ...");

		// 载入配置
		m_cfg.RegisterItem("TABLE", "TAB_YCRA_STATRULE");
		m_cfg.ReadConfig();
		std::string tab_rule = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_STATRULE");

		m_pAnaDB2->SetTabYCStatRule(tab_rule);
		m_pAnaDB2->SelectYCStatRule(m_taskInfo.KpiID, m_vecYCSInfo);
	}
}

void Analyse::DoDataAnalyse() throw(base::Exception)
{
	m_pLog->Output("[Analyse] 解析分析规则 ...");

	std::vector<std::string> vec_hivesql;
	AnalyseRules(vec_hivesql);

	m_pLog->Output("[Analyse] 获取Hive源数据 ...");
	FetchHiveSource(vec_hivesql);

	m_pLog->Output("[Analyse] 分析源数据 ...");
	AnalyseSourceData();

	m_pLog->Output("[Analyse] 生成结果数据 ...");
	StoreResult();

	m_pLog->Output("[Analyse] 告警判断 ...");
	AlarmJudgement();

	m_pLog->Output("[Analyse] 更新维度取值范围 ...");
	UpdateDimValue();
}

void Analyse::AnalyseRules(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	std::string& ref_exp = m_taskInfo.AnaRule.AnaExpress;
	base::PubStr::Trim(ref_exp);

	// 分析规则类型
	switch ( m_taskInfo.AnaRule.AnaType )
	{
	case AnalyseRule::ANATYPE_SUMMARY_COMPARE:		// 汇总对比
		m_pLog->Output("[Analyse] 分析规则类型：汇总对比 (KPI_ID:%s, ANA_ID:%s)", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetSummaryCompareHiveSQL(vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_DETAIL_COMPARE:		// 明细对比
		m_pLog->Output("[Analyse] 分析规则类型：明细对比 (KPI_ID:%s, ANA_ID:%s)", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetDetailCompareHiveSQL(vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_STATISTICS:			// 一般统计
		m_pLog->Output("[Analyse] 分析规则类型：一般统计 (KPI_ID:%s, ANA_ID:%s)", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetStatisticsHiveSQL(vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_REPORT_STATISTICS:	// 报表统计
		m_pLog->Output("[Analyse] 分析规则类型：报表统计 (KPI_ID:%s, ANA_ID:%s)", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetReportStatisticsHiveSQL(vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_HIVE_SQL:				// 可执行的HIVE SQL语句
		m_pLog->Output("[Analyse] 分析规则类型：可执行的HIVE SQL语句 (KPI_ID:%s, ANA_ID:%s)", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str());
		// 分析表达式，即是可执行的Hive SQL语句（可多个，以分号分隔）
		SplitHiveSqlExpress(ref_exp, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_YC_STAT:				// 业财稽核统计类型
		m_pLog->Output("[Analyse] 分析规则类型：业财稽核统计 (KPI_ID:%s, ANA_ID:%s)", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetStatisticsHiveSQL(vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_UNKNOWN:				// 未知类型
	default:
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "无法识别的分析规则类型: ANATYPE_UNKNOWN (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	// 生成数据库[DB2]信息
	GetAnaDBInfo();
}

// 暂时只支持两组数据的汇总对比
void Analyse::GetSummaryCompareHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckDualEtlRule(m_taskInfo.vecEtlRule) )
	{
	case -1:	// 采集规则个数不够
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "只有 %lu 份采集结果数据，无法进行汇总对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecEtlRule.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "采集规则的维度size不一致，无法进行汇总对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "采集规则的值size不一致，无法进行汇总对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 格式样例1: A-B, diff1:diff2:diff3:...:diffn		// 指定 1-n 列
	// 格式样例2: A-B, all								// 所有列
	std::string& ana_exp = m_taskInfo.AnaRule.AnaExpress;

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(ana_exp, ",", vec_str);
	if ( vec_str.size() != 2 )
	{
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	int first_index  = -1;
	int second_index = -1;
	DetermineDataGroup(vec_str[0], first_index, second_index);

	std::vector<int> vec_col;
	std::string diff_str = vec_str[1];
	const int VAL_SIZE = m_taskInfo.vecEtlRule[0].vecEtlVal.size();

	if ( base::PubStr::UpperB(diff_str) == "ALL" )		// 所有列
	{
		// 登记所有列序号
		for ( int i = 0; i < VAL_SIZE; ++i )
		{
			vec_col.push_back(i);
		}
	}
	else	// 指定列
	{
		base::PubStr::Str2StrVector(diff_str, ":", vec_str);
		if ( vec_str.empty() )
		{
			throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}

		std::set<int> set_diff;			// 用于查重
		const int DIFF_SIZE = vec_str.size();
		for ( int i = 0; i < DIFF_SIZE; ++i )
		{
			std::string& ref_str = vec_str[i];

			base::PubStr::Upper(ref_str);
			size_t pos = ref_str.find("DIFF");
			if ( std::string::npos == pos )
			{
				throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}

			int diff_no = -1;
			if ( !base::PubStr::T1TransT2(ref_str.substr(pos+4), diff_no) )
			{
				throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：[%s] 转换失败! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_str.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}

			if ( diff_no < 1 || diff_no > VAL_SIZE )
			{
				throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：列值 [%d] 越界! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", diff_no, m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}

			if ( set_diff.find(diff_no) != set_diff.end() )
			{
				throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：列值 [%d] 已经存在! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", diff_no, m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}

			set_diff.insert(diff_no);

			// 表达式中DIFF从1开始，而列序号是从0开始的
			vec_col.push_back(diff_no-1);
		}
	}

	std::vector<std::string> v_hive_sql;

	OneEtlRule& first_one  = m_taskInfo.vecEtlRule[first_index];
	OneEtlRule& second_one = m_taskInfo.vecEtlRule[second_index];

	// 检查：单独显示的字段是否对应
	if ( first_one.vecEtlSingleDim.size() != m_taskInfo.vecLeftKpiCol.size() )
	{
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "汇总对比：左侧，采集的单独显示字段数 [%lu] 与指标的单独显示字段数 [%lu] 不一致！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", first_one.vecEtlSingleDim.size(), m_taskInfo.vecLeftKpiCol.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}
	if ( second_one.vecEtlSingleDim.size() != m_taskInfo.vecRightKpiCol.size() )
	{
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "汇总对比：右侧，采集的单独显示字段数 [%lu] 与指标的单独显示字段数 [%lu] 不一致！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", second_one.vecEtlSingleDim.size(), m_taskInfo.vecRightKpiCol.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	//// 指定分析条件
	//const std::string CONDITION     = TaskInfoUtil::GetStraightAnaCondition(m_taskInfo.AnaRule.AnaCondType, m_taskInfo.AnaRule.AnaCondition, false);
	//const std::string ADD_CONDITION = TaskInfoUtil::GetStraightAnaCondition(m_taskInfo.AnaRule.AnaCondType, m_taskInfo.AnaRule.AnaCondition, true);

	// 1) 汇总：对平的Hive SQL语句
	std::string hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[0] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += " and " + TaskInfoUtil::GetCompareEqualValsByCol(first_one, second_one, vec_col) + ")";
	//TaskInfoUtil::AddConditionSql(hive_sql, CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 2) 汇总：有差异的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[1] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetCompareUnequalValsByCol(first_one, second_one, vec_col);
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 3) 汇总：取 A、B 两份汇总数据，（后续）找出非共有的数据
	hive_sql = TaskInfoUtil::GetOneEtlRuleDetailSQL(first_one);
	v_hive_sql.push_back(hive_sql);

	hive_sql = TaskInfoUtil::GetOneEtlRuleDetailSQL(second_one);
	v_hive_sql.push_back(hive_sql);

	v_hive_sql.swap(vec_hivesql);
}

// 暂时只支持两组数据的明细对比
void Analyse::GetDetailCompareHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckDualEtlRule(m_taskInfo.vecEtlRule) )
	{
	case -1:	// 采集规则个数不够
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "只有 %lu 份采集结果数据，无法进行明细对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecEtlRule.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "采集规则的维度size不一致，无法进行明细对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "采集规则的值size不一致，无法进行明细对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 格式样例: A-B
	// 暂时只支持这一种表达式："A-B"
	std::string& ana_exp = m_taskInfo.AnaRule.AnaExpress;

	int first_index  = -1;
	int second_index = -1;
	DetermineDataGroup(ana_exp, first_index, second_index);

	std::vector<int> vec_col;
	std::vector<std::string> v_hive_sql;

	OneEtlRule& first_one  = m_taskInfo.vecEtlRule[first_index];
	OneEtlRule& second_one = m_taskInfo.vecEtlRule[second_index];

	// 检查：单独显示的字段是否对应
	if ( first_one.vecEtlSingleDim.size() != m_taskInfo.vecLeftKpiCol.size() )
	{
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "明细对比：左侧，采集的单独显示字段数 [%lu] 与指标的单独显示字段数 [%lu] 不一致！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", first_one.vecEtlSingleDim.size(), m_taskInfo.vecLeftKpiCol.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}
	if ( second_one.vecEtlSingleDim.size() != m_taskInfo.vecRightKpiCol.size() )
	{
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "明细对比：右侧，采集的单独显示字段数 [%lu] 与指标的单独显示字段数 [%lu] 不一致！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", second_one.vecEtlSingleDim.size(), m_taskInfo.vecRightKpiCol.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	//// 指定分析条件
	//const std::string CONDITION     = TaskInfoUtil::GetStraightAnaCondition(m_taskInfo.AnaRule.AnaCondType, m_taskInfo.AnaRule.AnaCondition, false);
	//const std::string ADD_CONDITION = TaskInfoUtil::GetStraightAnaCondition(m_taskInfo.AnaRule.AnaCondType, m_taskInfo.AnaRule.AnaCondition, true);

	// 1) 明细：对平的Hive SQL语句
	std::string hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[0] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += " and " + TaskInfoUtil::GetCompareEqualVals(first_one, second_one) + ")";
	//TaskInfoUtil::AddConditionSql(hive_sql, CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 2) 明细：有差异的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[1] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetCompareUnequalVals(first_one, second_one);
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 3) 明细：左有右无的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFields(first_one, second_one);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[2] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a left outer join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetOneRuleValsNull(second_one, "b.");
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 4) 明细：左无右有的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFields(second_one, first_one, true);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[3] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + second_one.TargetPatch + " b left outer join " + first_one.TargetPatch;
	hive_sql += " a on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetOneRuleValsNull(first_one, "a.");
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	//// 3) 明细："左有右无"的数据，通过获取 A 和 B 两组数据分析得出
	//// 4) 明细："左无右有"的数据，通过获取 A 和 B 两组数据分析得出
	//hive_sql = TaskInfoUtil::GetOneEtlRuleDetailSQL(first_one);
	//v_hive_sql.push_back(hive_sql);

	//hive_sql = TaskInfoUtil::GetOneEtlRuleDetailSQL(second_one);
	//v_hive_sql.push_back(hive_sql);

	v_hive_sql.swap(vec_hivesql);
}

void Analyse::GetStatisticsHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckPluralEtlRule(m_taskInfo.vecEtlRule) )
	{
	case -1:	// 没有采集规则
		throw base::Exception(ANAERR_GET_STATISTICS_FAILED, "没有采集结果数据，无法进行一般统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecEtlRule.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_STATISTICS_FAILED, "采集规则的维度size不一致，无法进行一般统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_STATISTICS_FAILED, "采集规则的值size不一致，无法进行一般统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 当有多份采集结果数据时，采用联合语句
	bool is_union_all = (m_taskInfo.vecEtlRule.size() > 1);

	std::string& ana_exp = m_taskInfo.AnaRule.AnaExpress;
	if ( ana_exp.empty() )	// 没有指定表达式，则取全量数据
	{
		TaskInfoUtil::GetEtlStatisticsSQLs(m_taskInfo.vecEtlRule, vec_hivesql, is_union_all);
	}
	else
	{
		GetStatisticsHiveSQLBySet(vec_hivesql, is_union_all);
	}

	//AddAnalysisCondition(m_taskInfo.AnaRule, vec_hivesql);
}

void Analyse::GetReportStatisticsHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckPluralEtlRule(m_taskInfo.vecEtlRule) )
	{
	case -1:	// 没有采集规则
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "没有采集结果数据，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecEtlRule.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "采集规则的维度size不一致，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "采集规则的值size不一致，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 采集规则的值是否唯一
	// 由于上面已经确保了值的size都是一致的，所以检查第一组就可以了
	if ( m_taskInfo.vecEtlRule[0].vecEtlVal.size() != 1 )
	{
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "采集规则的值不唯一，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	std::string& ana_exp = m_taskInfo.AnaRule.AnaExpress;
	if ( ana_exp.empty() )	// 没有指定表达式，则取全量数据
	{
		TaskInfoUtil::GetEtlStatisticsSQLs(m_taskInfo.vecEtlRule, vec_hivesql, false);
	}
	else
	{
		GetStatisticsHiveSQLBySet(vec_hivesql, false);
	}

	//AddAnalysisCondition(m_taskInfo.AnaRule, vec_hivesql);
}

void Analyse::GetStatisticsHiveSQLBySet(std::vector<std::string>& vec_hivesql, bool union_all) throw(base::Exception)
{
	std::string& ana_exp = m_taskInfo.AnaRule.AnaExpress;

	std::set<int> set_int;
	const int RESULT = base::PubStr::Express2IntSet(ana_exp, set_int);

	if ( -1 == RESULT )		// 转换失败
	{
		throw base::Exception(ANAERR_GET_STAT_BY_SET_FAILED, "分析规则解析失败：分析规则表达式转换失败! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}
	else if ( 1 == RESULT )		// 数据重复
	{
		throw base::Exception(ANAERR_GET_STAT_BY_SET_FAILED, "分析规则解析失败：分析规则表达式存在重复数据! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}
	else
	{
		const int MAX_ETL_RULE_SIZE = m_taskInfo.vecEtlRule.size();

		// 检查合法性
		for ( std::set<int>::iterator it = set_int.begin(); it != set_int.end(); ++it )
		{
			const int& VAL = *it;

			if ( VAL < 1 || VAL > MAX_ETL_RULE_SIZE )
			{
				throw base::Exception(ANAERR_GET_STAT_BY_SET_FAILED, "分析规则解析失败：不存在指定的采集组 [%d] ! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", VAL, m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}
		}

		TaskInfoUtil::GetEtlStatisticsSQLsBySet(m_taskInfo.vecEtlRule, set_int, vec_hivesql, union_all);
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

void Analyse::GenerateTableNameByType() throw(base::Exception)
{
	AnaTaskInfo::ResultTableType& type = m_taskInfo.ResultType;

	// 取第一个采集规则的采集时间
	std::string& ref_etltime = m_taskInfo.vecEtlRule[0].EtlTime;

	base::PubTime::DATE_TYPE d_type;
	if ( !base::PubTime::DateApartFromNow(ref_etltime, d_type, m_dbinfo.date_time) )
	{
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "采集时间转换失败！无法识别的采集时间表达式：%s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_etltime.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	std::string tab_name = base::PubStr::TrimB(m_taskInfo.TableName);

	switch ( type )
	{
	case AnaTaskInfo::TABTYPE_COMMON:		// 普通表
		// Do nothing
		m_pLog->Output("[Analyse] 结果表类型：普通表");
		break;
	case AnaTaskInfo::TABTYPE_DAY:			// 天表
		if ( d_type > base::PubTime::DT_DAY )
		{
			throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "采集时间类型与结果表类型无法匹配！采集时间类型为：%s，结果表类型为：DAY_TABLE (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", base::PubTime::DateType2String(d_type).c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}
		else
		{
			tab_name = tab_name + "_" + m_dbinfo.date_time;
		}
		m_pLog->Output("[Analyse] 结果表类型：天表");
		break;
	case AnaTaskInfo::TABTYPE_MONTH:		// 月表
		if ( d_type > base::PubTime::DT_MONTH )
		{
			throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "采集时间类型与结果表类型无法匹配！采集时间类型为：%s，结果表类型为：MONTH_TABLE (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", base::PubTime::DateType2String(d_type).c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}
		else
		{
			tab_name = tab_name + "_" + m_dbinfo.date_time.substr(0, 6);
		}
		m_pLog->Output("[Analyse] 结果表类型：月表");
		break;
	case AnaTaskInfo::TABTYPE_YEAR:			// 年表
		tab_name = tab_name + "_" + m_dbinfo.date_time.substr(0, 4);
		m_pLog->Output("[Analyse] 结果表类型：年表");
		break;
	case AnaTaskInfo::TABTYPE_UNKNOWN:		// 未知类型
	default:
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "无法识别的目标表类型: TABTYPE_UNKNOWN (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	m_dbinfo.target_table = tab_name;
	m_pLog->Output("[Analyse] 最终目标表名：%s", tab_name.c_str());

	// 设置目标表的备份表名
	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == m_taskInfo.AnaRule.AnaType )		// 报表统计
	{
		m_dbinfo.backup_table = base::PubStr::UpperB(tab_name) + "_BAK";
		m_pLog->Output("[Analyse] (报表统计) 目标表的备份表名：%s", m_dbinfo.backup_table.c_str());
	}
	else	// 非报表统计
	{
		// 置空：非报表统计没有备份表
		m_dbinfo.backup_table.clear();
	}
}

void Analyse::AnalyseSourceData() throw(base::Exception)
{
	SrcDataUnifiedCoding();

	const int END_POS = m_dbinfo.val_beg_pos + m_taskInfo.vecKpiValCol.size();
	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == m_taskInfo.AnaRule.AnaType )		// 报表统计
	{
		m_pLog->Output("[Analyse] 报表统计类型数据转换 ...");

		m_pLog->Output("[Analyse] 转换前, 数据大小为: %llu", base::PubStr::CalcVVVectorStr(m_v3HiveSrcData));
		TransSrcDataToReportStatData();
		m_pLog->Output("[Analyse] 转换后, 数据大小为: %llu", m_v2ReportStatData.size());

		// 将源数据中的空值字符串("NULL")转换为("0")
		base::PubStr::ReplaceInStrVector2(m_v2ReportStatData, "NULL", "0", false, true);

		// 将数组中的带 'E' 的精度字符串转换为长精度字符串表示
		base::PubStr::TransVecDouStrWithE2LongDouStr(m_v2ReportStatData, m_dbinfo.val_beg_pos, END_POS);

		// 去除尾部的"."和其后的"0"
		base::PubStr::TrimTail0StrVec2(m_v2ReportStatData, m_dbinfo.val_beg_pos, END_POS);
	}
	else	// 非报表统计
	{
		//// 生成明细结果数据
		//CompareResultData();

		const int VEC3_SIZE = m_v3HiveSrcData.size();
		for ( int i = 0; i < VEC3_SIZE; ++i )
		{
			std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

			// 将源数据中的空值字符串("NULL")转换为("0")
			base::PubStr::ReplaceInStrVector2(ref_vec2, "NULL", "0", false, true);

			// 将数组中的带 'E' 的精度字符串转换为长精度字符串表示
			base::PubStr::TransVecDouStrWithE2LongDouStr(ref_vec2, m_dbinfo.val_beg_pos, END_POS);

			// 去除尾部的"."和其后的"0"
			base::PubStr::TrimTail0StrVec2(ref_vec2, m_dbinfo.val_beg_pos, END_POS);
		}

		AnalyseYCRAData();
	}

	m_pLog->Output("[Analyse] 分析完成!");
}

void Analyse::AnalyseYCRAData() throw(base::Exception)
{
	// 是否为业财稽核统计
	if ( AnalyseRule::ANATYPE_YC_STAT == m_taskInfo.AnaRule.AnaType )
	{
		if ( m_vecYCSInfo.empty() )
		{
			throw base::Exception(ANAERR_ANA_YCRA_DATA_FAILED, "没有业财稽核因子规则信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}

		std::map<std::string, double> map_src;
		TransYCStatFactor(map_src);

		GenerateYCResultData(map_src);
	}
}

void Analyse::TransYCStatFactor(std::map<std::string, double>& map_factor) throw(base::Exception)
{
	double yc_val = 0.0;
	std::string yc_dim;
	std::map<std::string, double> map_f;

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

		const int VEC2_SIZE = ref_vec2.size();
		for ( int j = 0; j < VEC2_SIZE; ++j )
		{
			std::vector<std::string>& ref_vec = ref_vec2[j];

			// 因子重复！
			yc_dim = base::PubStr::TrimUpperB(ref_vec[0]);
			if ( map_f.find(yc_dim) != map_f.end() )
			{
				throw base::Exception(ANAERR_TRANS_YCFACTOR_FAILED, "重复的业财稽核维度因子: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", yc_dim.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}

			// 维度值无法转换为精度型
			if ( !base::PubStr::T1TransT2(ref_vec[1], yc_val) )
			{
				throw base::Exception(ANAERR_TRANS_YCFACTOR_FAILED, "无效的业财稽核统计维度值: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_vec[1].c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}

			map_f[yc_dim] = yc_val;
		}
	}

	if ( map_f.empty() )
	{
		throw base::Exception(ANAERR_TRANS_YCFACTOR_FAILED, "没有业财稽核统计源数据! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	map_f.swap(map_factor);
	m_pLog->Output("[Analyse] 统计因子转换大小：%lu", map_factor.size());

	// 释放Hive源数据
	std::vector<std::vector<std::vector<std::string> > >().swap(m_v3HiveSrcData);
}

void Analyse::GenerateYCResultData(std::map<std::string, double>& map_factor) throw(base::Exception)
{
	YCStatResult yc_sr;
	std::vector<std::string> v_dat;
	std::vector<std::vector<std::string> > vec_yc_data;
	std::map<std::string, double>::iterator m_it;

	// 生成结果数据
	const int VEC_YCSI_SIZE = m_vecYCSInfo.size();
	for ( int i = 0; i < VEC_YCSI_SIZE; ++i )
	{
		YCStatInfo& ref_ycsi = m_vecYCSInfo[i];
		base::PubStr::TrimUpper(ref_ycsi.statdim_id);
		m_pLog->Output("[Analyse] [STAT_ID:%s, STATDIM_ID:%s, STAT_PRIORITY:%d] 正在生成统计因子结果数据...", ref_ycsi.stat_id.c_str(), ref_ycsi.statdim_id.c_str(), ref_ycsi.stat_pri);

		if ( YCStatInfo::SP_Level_0 == ref_ycsi.stat_pri )
		{
			m_pLog->Output("[Analyse] 统计因子类型：一般因子");

			yc_sr.stat_report = ref_ycsi.stat_report;
			yc_sr.stat_id     = ref_ycsi.stat_id;
			yc_sr.stat_name   = ref_ycsi.stat_name;
			yc_sr.statdim_id  = ref_ycsi.statdim_id;

			if ( (m_it = map_factor.find(ref_ycsi.statdim_id)) != map_factor.end() )
			{
				yc_sr.stat_value = m_it->second;
			}
			else
			{
				throw base::Exception(ANAERR_GENERATE_YCDATA_FAILED, "不存在的业财稽核统计维度ID: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_ycsi.statdim_id.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}

			yc_sr.Trans2Vector(v_dat);
			base::PubStr::VVectorSwapPushBack(vec_yc_data, v_dat);
		}
		else if ( YCStatInfo::SP_Level_0 == ref_ycsi.stat_pri )
		{
			m_pLog->Output("[Analyse] 统计因子类型：组合因子");

			yc_sr.stat_report = ref_ycsi.stat_report;
			yc_sr.stat_id     = ref_ycsi.stat_id;
			yc_sr.stat_name   = ref_ycsi.stat_name;
			yc_sr.statdim_id  = ref_ycsi.statdim_id;
			yc_sr.stat_value  = CalcYCComplexFactor(map_factor, ref_ycsi.stat_sql);

			yc_sr.Trans2Vector(v_dat);
			base::PubStr::VVectorSwapPushBack(vec_yc_data, v_dat);
		}
		else
		{
			throw base::Exception(ANAERR_GENERATE_YCDATA_FAILED, "未知的统计因子优先级别！(KPI_ID:%s, ANA_ID:%s, STATDIM_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), ref_ycsi.statdim_id.c_str(), __FILE__, __LINE__);
		}
	}

	// 插入业财稽核结果数据
	base::PubStr::VVVectorSwapPushBack(m_v3HiveSrcData, vec_yc_data);
}

double Analyse::CalcYCComplexFactor(std::map<std::string, double>& map_factor, const std::string& cmplx_factr_fmt) throw(base::Exception)
{
	m_pLog->Output("[Analyse] 组合因子表达式：%s", cmplx_factr_fmt.c_str());

	// 组合因子格式：[ A1, A2, A3, ...|+, -, ... ]
	std::vector<std::string> vec_cf_1;
	base::PubStr::Str2StrVector(cmplx_factr_fmt, "|", vec_cf_1);
	if ( vec_cf_1.size() != 2 )
	{
		throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "无法识别的组合因子表达式：%s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", cmplx_factr_fmt.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	std::string yc_dims = base::PubStr::TrimUpperB(vec_cf_1[0]);
	std::string yc_oper = base::PubStr::TrimUpperB(vec_cf_1[1]);

	std::vector<std::string> vec_cf_2;
	base::PubStr::Str2StrVector(yc_dims, ",", vec_cf_1);
	base::PubStr::Str2StrVector(yc_oper, ",", vec_cf_2);

	// 至少两个统计维度；且运算符个数比维度少一个
	const int VEC_CF_SIZE = vec_cf_1.size();
	if ( VEC_CF_SIZE < 2 || (size_t)VEC_CF_SIZE != (vec_cf_2.size() + 1) )
	{
		throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "不匹配的组合因子表达式：%s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", cmplx_factr_fmt.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
	}

	// 计算结果
	std::map<std::string, double>::iterator m_it;
	double cmplx_factr_result = 0.0;
	for ( int i = 1; i < VEC_CF_SIZE; ++i )
	{
		if ( 1 == i )	// 首次
		{
			std::string& ref_dim_0 = vec_cf_1[0];
			if ( (m_it = map_factor.find(ref_dim_0)) != map_factor.end() )
			{
				cmplx_factr_result += m_it->second;
			}
			else
			{
				throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "不存在的业财稽核统计维度ID: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_dim_0.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}
		}

		std::string& ref_dim = vec_cf_1[i];
		if ( (m_it = map_factor.find(ref_dim)) != map_factor.end() )
		{
			std::string& ref_oper = vec_cf_2[i-1];
			if ( "+" == ref_oper )
			{
				cmplx_factr_result += m_it->second;
			}
			else if ( "-" == ref_oper )
			{
				cmplx_factr_result -= m_it->second;
			}
			else if ( "*" == ref_oper )
			{
				cmplx_factr_result *= m_it->second;
			}
			else if ( "/" == ref_oper )
			{
				cmplx_factr_result /= m_it->second;
			}
			else
			{
				throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "无法识别的组合因子运算符: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_oper.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
			}
		}
		else
		{
			throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "不存在的业财稽核统计维度ID: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_dim.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
		}
	}

	return cmplx_factr_result;
}

void Analyse::SrcDataUnifiedCoding() throw(base::Exception)
{
	const int DIM_REGION_INDEX  = m_taskInfo.GetDimRegionIndex();
	const int DIM_CHANNEL_INDEX = m_taskInfo.GetDimChannelIndex();

	std::set<std::string> set_not_exist_region;			// 未成功转换的地市别名
	std::set<std::string> set_not_exist_channel;		// 未成功转换的渠道别名

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

		const size_t VEC2_SIZE = ref_vec2.size();
		for ( size_t j = 0; j < VEC2_SIZE; ++j )
		{
			std::vector<std::string>& ref_vec1 = ref_vec2[j];

			const int VEC1_SIZE = ref_vec1.size();
			if ( DIM_REGION_INDEX != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在-地市维度
			{
				if ( DIM_REGION_INDEX >= VEC1_SIZE )	// 地市维度位置索引越界？
				{
					throw base::Exception(ANAERR_SRC_DATA_UNIFIED_CODE, "The dim region index (%d) is out of range in HIVE source data (VEC_3:%d, VEC_2:%llu) [FILE:%s, LINE:%d]", DIM_REGION_INDEX, i, j, __FILE__, __LINE__);
				}
				else
				{
					std::string& ref_str = ref_vec1[DIM_REGION_INDEX];

					// 地市统一编码转换不成功
					if ( !m_UniCodeTransfer.RegionTransfer(ref_str, ref_str) )
					{
						set_not_exist_region.insert(ref_str);
					}
				}
			}

			if ( DIM_CHANNEL_INDEX != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在-渠道维度
			{
				if ( DIM_CHANNEL_INDEX >= VEC1_SIZE )	// 渠道维度位置索引越界？
				{
					throw base::Exception(ANAERR_SRC_DATA_UNIFIED_CODE, "The dim channel index (%d) is out of range in HIVE source data (VEC_3:%d, VEC_2:%llu) [FILE:%s, LINE:%d]", DIM_CHANNEL_INDEX, i, j, __FILE__, __LINE__);
				}
				else
				{
					std::string& ref_str = ref_vec1[DIM_CHANNEL_INDEX];

					// 渠道统一编码转换不成功
					if ( !m_UniCodeTransfer.ChannelTransfer(ref_str, ref_str) )
					{
						set_not_exist_channel.insert(ref_str);
					}
				}
			}
		}
	}

	// 输出转换不成功的地市别名
	std::set<std::string>::iterator s_it = set_not_exist_region.begin();
	for ( ; s_it != set_not_exist_region.end(); ++s_it )
	{
		m_pLog->Output("<WARNING> [Analyse] 不存在对应地市统一编码的地市别名：[%s]", s_it->c_str());
	}

	// 输出转换不成功的渠道别名
	for ( s_it = set_not_exist_channel.begin(); s_it != set_not_exist_channel.end(); ++s_it )
	{
		m_pLog->Output("<WARNING> [Analyse] 不存在对应渠道统一编码的渠道别名：[%s]", s_it->c_str());
	}
}

void Analyse::TransSrcDataToReportStatData()
{
	std::map<std::string, std::vector<std::string> > mReportStatData;
	std::map<std::string, std::vector<std::string> >::iterator it = mReportStatData.end();

	std::string str_tmp;
	std::string m_key;

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

		const size_t VEC2_SIZE = ref_vec2.size();
		for ( size_t j = 0; j < VEC2_SIZE; ++j )
		{
			std::vector<std::string>& ref_vec1 = ref_vec2[j];

			// 组织key值
			m_key.clear();
			const int VEC1_SIZE = ref_vec1.size();
			const int DIM_SIZE = VEC1_SIZE - 1;
			for ( int k = 0; k < DIM_SIZE; ++k )
			{
				std::string& ref_str = ref_vec1[k];
				m_key += ref_str;
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
				ref_vec1[DIM_SIZE] = "NULL";
				//ref_vec1[DIM_SIZE] = "";		// 置为空

				const int TOTAL_SIZE = VEC3_SIZE + VEC1_SIZE - 1;
				for ( int l = VEC1_SIZE; l < TOTAL_SIZE; ++l )
				{
					ref_vec1.push_back("NULL");
					//ref_vec1.push_back("");		// 置为空
				}
				ref_vec1[VAL_INDEX] = str_tmp;

				mReportStatData[m_key].swap(ref_vec1);
			}
		}
	}

	std::vector<std::vector<std::string> > vec2_reportdata;
	for ( it = mReportStatData.begin(); it != mReportStatData.end(); ++it )
	{
		base::PubStr::VVectorSwapPushBack(vec2_reportdata, it->second);
	}
	vec2_reportdata.swap(m_v2ReportStatData);

	// 释放Hive源数据
	std::vector<std::vector<std::vector<std::string> > >().swap(m_v3HiveSrcData);
}

//void Analyse::CompareResultData() throw(base::Exception)
//{
//	// 是否为对比分析类型？
//	if ( AnalyseRule::ANATYPE_DETAIL_COMPARE == m_taskInfo.AnaRule.AnaType 
//		|| AnalyseRule::ANATYPE_SUMMARY_COMPARE == m_taskInfo.AnaRule.AnaType )
//	{
//		if ( m_v3HiveSrcData.size() != COMPARE_HIVE_SRCDATA_SIZE )
//		{
//			throw base::Exception(ANAERR_COMPARE_RESULT_DATA, "不正确的对比源数据个数: %lu (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_v3HiveSrcData.size(), m_taskInfo.KpiID.c_str(), m_taskInfo.AnaRule.AnaID.c_str(), __FILE__, __LINE__);
//		}
//
//		std::string com_desc;
//		std::string left_desc;
//		std::string right_desc;
//		if ( AnalyseRule::ANATYPE_DETAIL_COMPARE == m_taskInfo.AnaRule.AnaType )	// 明细
//		{
//			com_desc   = "明细";
//			left_desc  = m_taskInfo.vecComResDesc[2];
//			right_desc = m_taskInfo.vecComResDesc[3];
//			m_pLog->Output("[Analyse] 进行 \"%s\" 和 \"%s\" 的明细数据对比 ...", left_desc.c_str(), right_desc.c_str());
//		}
//		else	// 汇总
//		{
//			com_desc   = "汇总";
//			left_desc  = m_taskInfo.vecComResDesc[1];
//			right_desc = m_taskInfo.vecComResDesc[1];
//			m_pLog->Output("[Analyse] 进行 \"%s\" 的汇总数据对比 ...", left_desc.c_str());
//		}
//
//		// 维度个数与值的开始序号一致
//		const int DIM_SIZE = m_dbinfo.val_beg_pos;
//		const int VAL_SIZE = m_taskInfo.vecEtlRule[0].vecEtlVal.size();
//
//		const int LEFT_SINGLE_DIM_SIZE  = m_taskInfo.vecLeftKpiCol.size();
//		const int RIGHT_SINGLE_DIM_SIZE = m_taskInfo.vecRightKpiCol.size();
//
//		CompareResult com_result;
//		m_pLog->Output("[Analyse] 录入两组原始%s数据 ...", com_desc.c_str());
//		ComDataIndex left_index  = com_result.SetCompareData(m_v3HiveSrcData[2], DIM_SIZE, VAL_SIZE, LEFT_SINGLE_DIM_SIZE);
//		m_pLog->Output("[Analyse] 第 1 组原始%s数据录入完成, 录入 size: %llu", com_desc.c_str(), m_v3HiveSrcData[2].size());
//		ComDataIndex right_index = com_result.SetCompareData(m_v3HiveSrcData[3], DIM_SIZE, VAL_SIZE, RIGHT_SINGLE_DIM_SIZE);
//		m_pLog->Output("[Analyse] 第 2 组原始%s数据录入完成, 录入 size: %llu", com_desc.c_str(), m_v3HiveSrcData[3].size());
//
//		// 删除 A、B 两组原始数据
//		m_pLog->Output("[Analyse] 删除两组原始%s数据 ...", com_desc.c_str());
//		m_v3HiveSrcData.erase(m_v3HiveSrcData.begin() + 3);
//		m_v3HiveSrcData.erase(m_v3HiveSrcData.begin() + 2);
//
//		std::vector<std::vector<std::string> > vec2_result;
//		// "左有右无" 的对比结果数据
//		m_pLog->Output("[Analyse] 生成%s对比 \"%s\" 的结果数据 ...", com_desc.c_str(), left_desc.c_str());
//		com_result.GetCompareResult(left_index, right_index, CompareResult::CTYPE_LEFT, left_desc, vec2_result);
//		m_pLog->Output("[Analyse] 成功生成%s对比 \"%s\" 的结果数据, size: %llu", com_desc.c_str(), left_desc.c_str(), vec2_result.size());
//		base::PubStr::VVVectorSwapPushBack(m_v3HiveSrcData, vec2_result);
//
//		// "左无右有" 的对比结果数据
//		m_pLog->Output("[Analyse] 生成%s对比 \"%s\" 的结果数据 ...", com_desc.c_str(), right_desc.c_str());
//		com_result.GetCompareResult(left_index, right_index, CompareResult::CTYPE_RIGHT, right_desc, vec2_result);
//		m_pLog->Output("[Analyse] 成功生成%s对比 \"%s\" 的结果数据, size: %llu", com_desc.c_str(), right_desc.c_str(), vec2_result.size());
//		base::PubStr::VVVectorSwapPushBack(m_v3HiveSrcData, vec2_result);
//	}
//}

void Analyse::CollectDimVal()
{
	m_pLog->Output("[Analyse] 收集源数据的所有维度取值 ...");

	DimVal dv;
	dv.KpiID = m_taskInfo.KpiID;

	std::vector<KpiColumn>& v_dimcol = m_taskInfo.vecKpiDimCol;
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

void Analyse::StoreResult() throw(base::Exception)
{
	// 删除旧的数据
	RemoveOldResult(m_taskInfo.ResultType);

	// 是否为报表统计类型
	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == m_taskInfo.AnaRule.AnaType )	// 报表统计类型
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

void Analyse::RemoveOldResult(const AnaTaskInfo::ResultTableType& result_tabtype) throw(base::Exception)
{
	// 是否带时间戳
	// 只有带时间戳才可以按采集时间删除结果数据
	if ( m_dbinfo.time_stamp )
	{
		// 结果表类型是否为天表？
		if ( AnaTaskInfo::TABTYPE_DAY == result_tabtype )
		{
			m_pLog->Output("[Analyse] 清空天表数据 ...");
			m_pAnaDB2->DeleteResultData(m_dbinfo, true);
		}
		else
		{
			m_pLog->Output("[Analyse] 删除已存在的结果数据 ...");
			m_pAnaDB2->DeleteResultData(m_dbinfo, false);
		}
	}
	else
	{
		m_pLog->Output("[Analyse] 无法按时间区分结果数据，因此不进行删除操作！");
	}
}

void Analyse::AlarmJudgement() throw(base::Exception)
{
	// 业财稽核统计，暂不生成告警！
	if ( AnalyseRule::ANATYPE_YC_STAT == m_taskInfo.AnaRule.AnaType )
	{
		return;
	}

	// 是否有配置告警？
	if ( m_taskInfo.vecAlarm.empty() )
	{
		m_pLog->Output("[Analyse] 无告警配置：不产生告警!");
		return;
	}

	const int VEC_SIZE = m_taskInfo.vecAlarm.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		AlarmRule& ref_rule = m_taskInfo.vecAlarm[i];
		m_pLog->Output("[Analyse] 告警判断：告警 %d <ID:%s, NAME:%d>", (i+1), ref_rule.AlarmID.c_str(), ref_rule.AlarmName.c_str());

		if ( ref_rule.AlarmExpress.empty() )
		{
			throw base::Exception(ANAERR_ALARM_JUDGEMENT_FAILED, "告警规则表达式缺失！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), ref_rule.AlarmID.c_str(), __FILE__, __LINE__);
		}

		switch ( ref_rule.AlarmType )
		{
		case AlarmRule::AT_FLUCTUATE:
			m_pLog->Output("[Analyse] 告警类型：波动告警");
			FluctuateAlarm(ref_rule);
			break;
		case AlarmRule::AT_RATIO:
			m_pLog->Output("[Analyse] 告警类型：对比告警");
			RatioAlarm(ref_rule);
			break;
		case AlarmRule::AT_UNKNOWN:
		default:
			throw base::Exception(ANAERR_ALARM_JUDGEMENT_FAILED, "无法识别的告警规则类型：AT_UNKNOWN！(KPI_ID:%s, ALARM_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), ref_rule.AlarmID.c_str(), __FILE__, __LINE__);
			break;
		}
	}
}

void Analyse::FluctuateAlarm(AlarmRule& alarm_rule) throw(base::Exception)
{
	AlarmFluctuate alarm_flu;
	alarm_flu.SetTaskDBInfo(m_taskInfo, m_dbinfo);
	alarm_flu.SetAlarmRule(alarm_rule);
	alarm_flu.SetUnicodeTransfer(m_UniCodeTransfer);
	alarm_flu.AnalyseExpression();

	const std::string ALARM_DATE = alarm_flu.GetFluctuateDate();
	m_pLog->Output("[Analyse] 准备目标表(%s)的比对数据 (时间:%s) ...", m_dbinfo.target_table.c_str(), ALARM_DATE.c_str());
	std::vector<std::vector<std::string> > vec2_old_data;
	m_pAnaDB2->SelectTargetData(m_dbinfo, ALARM_DATE, vec2_old_data);
	m_pLog->Output("[Analyse] 获得比对数据，大小为：%llu", vec2_old_data.size());

	alarm_flu.SetCompareData(vec2_old_data);

	// 是否为报表数据
	if ( m_taskInfo.AnaRule.AnaType != AnalyseRule::ANATYPE_REPORT_STATISTICS )	// 非报表数据
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

void Analyse::RatioAlarm(AlarmRule& alarm_rule) throw(base::Exception)
{
	AlarmRatio alarm_rat;
	alarm_rat.SetTaskDBInfo(m_taskInfo, m_dbinfo);
	alarm_rat.SetAlarmRule(alarm_rule);
	alarm_rat.SetUnicodeTransfer(m_UniCodeTransfer);
	alarm_rat.AnalyseExpression();

	// 是否为报表数据
	if ( m_taskInfo.AnaRule.AnaType != AnalyseRule::ANATYPE_REPORT_STATISTICS )	// 非报表数据
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
	if ( alarm_rat.GenerateAlarmEvent(vec_event) )		// 产生告警事件
	{
		HandleAlarmEvent(vec_event);
	}
	else		// 无告警事件
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

