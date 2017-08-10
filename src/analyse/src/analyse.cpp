#include "analyse.h"
#include <vector>
#include <set>
#include <map>
#include "log.h"
#include "autodisconnect.h"
#include "pubstr.h"
#include "canadb2.h"
#include "canahive.h"
#include "taskinfoutil.h"
#include "sqltranslator.h"
//#include "compareresult.h"
#include "reportconverter.h"
#include "reportinput.h"
#include "reportoutput.h"


Analyse::Analyse()
:m_pAnaDB2(NULL)
,m_pAnaHive(NULL)
,m_pSQLTranslator(NULL)
{
}

Analyse::~Analyse()
{
	ReleaseSQLTranslator();
}

const char* Analyse::Version()
{
	return ("Analyse: Version 5.1.5.2 released. Compiled at " __TIME__ " on " __DATE__);
}

void Analyse::LoadConfig() throw(base::Exception)
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("SYS", "JVM_INIT_MEM_SIZE");
	m_cfg.RegisterItem("SYS", "JVM_MAX_MEM_SIZE");
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
	m_cfg.RegisterItem("TABLE", "TAB_DICT_CHANNEL");
	m_cfg.RegisterItem("TABLE", "TAB_DICT_CITY");

	m_cfg.ReadConfig();

	// 设置 Java 虚拟机初始化内存大小 (MB)
	base::BaseJHive::SetJVMInitMemSize(m_cfg.GetCfgUIntVal("SYS", "JVM_INIT_MEM_SIZE"));
	// 设置 Java 虚拟机最大内存大小 (MB)
	base::BaseJHive::SetJVMMaxMemSize(m_cfg.GetCfgUIntVal("SYS", "JVM_MAX_MEM_SIZE"));

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
	m_tabDictChannel = m_cfg.GetCfgValue("TABLE", "TAB_DICT_CHANNEL");
	m_tabDictCity    = m_cfg.GetCfgValue("TABLE", "TAB_DICT_CITY");

	m_pLog->Output("[Analyse] Load configuration OK.");
}

void Analyse::Init() throw(base::Exception)
{
	GetParameterTaskInfo(GetTaskParaInfo());

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
	m_pAnaDB2->SetTabDictChannel(m_tabDictChannel);
	m_pAnaDB2->SetTabDictCity(m_tabDictCity);

	// 连接数据库
	m_pAnaDB2->Connect();

	if ( m_isTest )		// 测试环境
	{
		m_pHive = new CAnaHive(base::BaseJHive::S_DEBUG_HIVE_JAVA_CLASS_NAME);
	}
	else	// 非测试环境
	{
		m_pHive = new CAnaHive(base::BaseJHive::S_RELEASE_HIVE_JAVA_CLASS_NAME);
	}

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
	// 断开数据库连接
	if ( m_pAnaDB2 != NULL )
	{
		m_pAnaDB2->Disconnect();
	}
}

void Analyse::ReleaseSQLTranslator()
{
	if ( m_pSQLTranslator != NULL )
	{
		delete m_pSQLTranslator;
		m_pSQLTranslator = NULL;
	}
}

void Analyse::GetParameterTaskInfo(const std::string& para) throw(base::Exception)
{
	// 格式：启动批号:指标ID:分析规则ID:...
	VEC_STRING vec_str;
	base::PubStr::Str2StrVector(para, ":", vec_str);

	if ( vec_str.size() < 3 )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法按格式拆分! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	m_sKpiID = base::PubStr::TrimB(vec_str[1]);
	if ( m_sKpiID.empty() )
	{
		throw base::Exception(ANAERR_KPIID_INVALID, "指标ID无效! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_sAnaID = base::PubStr::TrimB(vec_str[2]);
	if ( m_sAnaID.empty() )
	{
		throw base::Exception(ANAERR_ANAID_INVALID, "分析规则ID无效! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[Analyse] 任务参数信息：指标ID [KPI_ID:%s], 分析规则ID [ANA_ID:%s]", m_sKpiID.c_str(), m_sAnaID.c_str());

	GetExtendParaTaskInfo(vec_str);
}

void Analyse::GetAnaDBInfo() throw(base::Exception)
{
	AnaField ana_field;
	std::vector<AnaField> v_fields;

	// 指标维度字段
	int v_size = m_taskInfo.vecKpiDimCol.size();
	for ( int i = 0; i < v_size; ++i )
	{
		KpiColumn& ref_col = m_taskInfo.vecKpiDimCol[i];

		if ( ref_col.DBName.empty() )
		{
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "维度字段名为空! 无效! (KPI_ID=%s, COL_TYPE=CTYPE_DIM, COL_SEQ=%d) [FILE:%s, LINE:%d]", ref_col.KpiID.c_str(), ref_col.ColSeq, __FILE__, __LINE__);
		}

		if ( ref_col.ColSeq < 0 )
		{
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "无法识别的维度字段序号: %d (KPI_ID=%s, COL_TYPE=CTYPE_DIM, DB_NAME=%s) [FILE:%s, LINE:%d]", ref_col.ColSeq, ref_col.KpiID.c_str(), ref_col.DBName.c_str(), __FILE__, __LINE__);
		}

		if ( KpiColumn::EWTYPE_ETLDAY == ref_col.ExpWay )
		{
			m_dbinfo.SetEtlDayIndex(i);
		}
		else if ( KpiColumn::EWTYPE_NOWDAY == ref_col.ExpWay )
		{
			m_dbinfo.SetNowDayIndex(i);
		}

		ana_field.field_name = ref_col.DBName;
		ana_field.CN_name    = ref_col.CNName;
		v_fields.push_back(ana_field);
	}

	// 指标值字段
	std::string error_msg;
	if ( !TaskInfoUtil::KpiColumnPushBackAnaFields(v_fields, m_taskInfo.vecKpiValCol, error_msg) )
	{
		throw base::Exception(ANAERR_GET_DBINFO_FAILED, "指标值：%s [FILE:%s, LINE:%d]", error_msg.c_str(), __FILE__, __LINE__);
	}

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
	m_dbinfo.SetAnaFields(v_fields);
}

void Analyse::ExchangeSQLMark(std::string& sql) throw(base::Exception)
{
	if ( NULL == m_pSQLTranslator )
	{
		throw base::Exception(ANAERR_EXCHG_SQLMARK_FAILED, "Exchange sql mark failed: NO SQL Translator! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	std::string str_error;
	if ( !m_pSQLTranslator->Translate(sql, &str_error) )
	{
		throw base::Exception(ANAERR_EXCHG_SQLMARK_FAILED, "Exchange sql mark failed: %s [FILE:%s, LINE:%d]", str_error.c_str(), __FILE__, __LINE__);
	}
}

void Analyse::FetchHiveSource(VEC_STRING& vec_hivesql) throw(base::Exception)
{
	VEC2_STRING vec2_fields;

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
	CollectDimVal();

	m_pAnaDB2->SelectDimValue(m_sKpiID, m_DVDiffer);
	m_pLog->Output("[Analyse] 从数据库中获取指标 (KPI_ID:%s) 的维度取值范围, size: %llu", m_sKpiID.c_str(), m_DVDiffer.GetDBDimValSize());

	std::vector<DimVal> vec_diff_dv;
	m_DVDiffer.GetDimValDiff(vec_diff_dv);

	const size_t VEC_DV = vec_diff_dv.size();
	m_pLog->Output("[Analyse] 需要更新的维度取值数：%llu", VEC_DV);

	if ( VEC_DV > 0 )
	{
		m_pLog->Output("[Analyse] 详细的需要更新的维度取值如下：");
		for ( size_t s = 0; s < VEC_DV; ++s )
		{
			DimVal& ref_dv = vec_diff_dv[s];
			m_pLog->Output("[Analyse] 维度取值 [%llu]：KPI=[%s], DB_NAME=[%s], VALUE=[%s]", (s+1), ref_dv.KpiID.c_str(), ref_dv.DBName.c_str(), ref_dv.Value.c_str());
		}

		m_pAnaDB2->InsertNewDimValue(vec_diff_dv);
		m_pLog->Output("[Analyse] 更新维度取值范围成功！");
	}
	else
	{
		m_pLog->Output("[Analyse] 无需更新维度取值范围.");
	}
}

//void Analyse::AddAnalysisCondition(AnalyseRule& ana_rule, VEC_STRING& vec_sql)
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

	EtlTimeConvertion();
}

void Analyse::CheckAnaTaskInfo() throw(base::Exception)
{
	if ( AnaTaskInfo::TABTYPE_UNKNOWN == m_taskInfo.ResultType )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "未知的分析结果表类型! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	if ( m_taskInfo.TableName.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "分析结果表为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	if ( AnalyseRule::ANATYPE_UNKNOWN == m_taskInfo.AnaRule.AnaType )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "未知的分析规则类型! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	if ( AnalyseRule::ANATYPE_SUMMARY_COMPARE == m_taskInfo.AnaRule.AnaType )
	{
		// 汇总对比的结果描述共 2 个：对平，有差异
		if ( m_taskInfo.vecComResDesc.size() < (size_t)2 )
		{
			throw base::Exception(ANAERR_TASKINFO_INVALID, "汇总对比缺少结果描述，当前结果描述个数：%lu (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecComResDesc.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}
	}
	else if ( AnalyseRule::ANATYPE_DETAIL_COMPARE == m_taskInfo.AnaRule.AnaType )
	{
		// 明细对比的结果描述共 4 个：对平，有差异，左有右无，左无右有
		if ( m_taskInfo.vecComResDesc.size() < (size_t)4 )
		{
			throw base::Exception(ANAERR_TASKINFO_INVALID, "明细对比缺少结果描述，当前结果描述个数：%lu (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecComResDesc.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}
	}

	// 分析规则表达式可能为空！
	//if ( m_taskInfo.AnaRule.AnaExpress.empty() )
	//{
	//	throw base::Exception(ANAERR_TASKINFO_INVALID, "分析规则表达式为空! 无效! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	//}

	if ( m_taskInfo.vecEtlRule.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有采集规则信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	if ( m_taskInfo.vecKpiDimCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标维度信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	if ( m_taskInfo.vecKpiValCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标值信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	//if ( AnalyseRule::ACTYPE_UNKNOWN == m_taskInfo.AnaRule.AnaCondType )
	//{
	//	throw base::Exception(ANAERR_TASKINFO_INVALID, "未知的分析条件类型! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	//}

	CheckExpWayType();

	m_pLog->Output("[Analyse] 分析类型：[%s] (%s)", m_sType.c_str(), (m_isTest ? "测试":"发布"));
}

void Analyse::CheckExpWayType() throw(base::Exception)
{
	// 检查维度：是否存在重复的 '地市' 和 '渠道' 表示类型
	bool etlday_existed  = false;
	bool nowday_existed  = false;
	bool region_existed  = false;
	bool channel_existed = false;
	int vec_size = m_taskInfo.vecKpiDimCol.size();
	for ( int i = 0; i < vec_size; ++i )
	{
		KpiColumn& ref_kpi_dim = m_taskInfo.vecKpiDimCol[i];

		if ( KpiColumn::EWTYPE_ETLDAY == ref_kpi_dim.ExpWay )	// 采集时间
		{
			if ( etlday_existed )	// 采集时间已经存在
			{
				throw base::Exception(ANAERR_TASKINFO_INVALID, "采集时间维度字段重复配置！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}
			else
			{
				etlday_existed = true;
			}
		}
		else if ( KpiColumn::EWTYPE_NOWDAY == ref_kpi_dim.ExpWay )	// 当前时间
		{
			if ( nowday_existed )	// 当前时间已经存在
			{
				throw base::Exception(ANAERR_TASKINFO_INVALID, "当前时间维度字段重复配置！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}
			else
			{
				nowday_existed = true;
			}
		}
		else if ( KpiColumn::EWTYPE_REGION == ref_kpi_dim.ExpWay )	// 地市
		{
			if ( region_existed )	// 地市已经存在
			{
				throw base::Exception(ANAERR_TASKINFO_INVALID, "地市维度字段重复配置！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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
				throw base::Exception(ANAERR_TASKINFO_INVALID, "渠道维度字段重复配置！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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
				throw base::Exception(ANAERR_TASKINFO_INVALID, "对比结果值字段重复配置！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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

void Analyse::EtlTimeConvertion() throw(base::Exception)
{
	// 取第一个采集规则的采集时间
	const std::string& REF_ETLTIME = m_taskInfo.vecEtlRule[0].EtlTime;
	if ( !m_dbinfo.GenerateDayTime(REF_ETLTIME) )
	{
		throw base::Exception(ANAERR_ETLTIME_CONVERTION, "采集时间转换失败！无法识别的采集时间表达式：%s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", REF_ETLTIME.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Analyse] 完成采集时间转换：[%s] -> [%s]", REF_ETLTIME.c_str(), m_dbinfo.GetEtlDay().c_str());
}

void Analyse::DoDataAnalyse() throw(base::Exception)
{
	m_pLog->Output("[Analyse] 解析分析规则 ...");

	VEC_STRING vec_hivesql;
	AnalyseRules(vec_hivesql);

	m_pLog->Output("[Analyse] 获取Hive源数据 ...");
	FetchHiveSource(vec_hivesql);

	m_pLog->Output("[Analyse] 分析源数据 ...");
	AnalyseSourceData();

	m_pLog->Output("[Analyse] 生成结果数据 ...");
	StoreResult();

	m_pLog->Output("[Analyse] 更新维度取值范围 ...");
	UpdateDimValue();
}

void Analyse::AnalyseRules(VEC_STRING& vec_hivesql) throw(base::Exception)
{
	base::PubStr::Trim(m_taskInfo.AnaRule.AnaExpress);
	std::string& ref_exp = m_taskInfo.AnaRule.AnaExpress;

	// 分析规则类型
	switch ( m_taskInfo.AnaRule.AnaType )
	{
	case AnalyseRule::ANATYPE_SUMMARY_COMPARE:		// 汇总对比
		m_pLog->Output("[Analyse] 分析规则类型：汇总对比 (KPI_ID:%s, ANA_ID:%s)", m_sKpiID.c_str(), m_sAnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetSummaryCompareHiveSQL(vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_DETAIL_COMPARE:		// 明细对比
		m_pLog->Output("[Analyse] 分析规则类型：明细对比 (KPI_ID:%s, ANA_ID:%s)", m_sKpiID.c_str(), m_sAnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetDetailCompareHiveSQL(vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_STATISTICS:			// 一般统计
		m_pLog->Output("[Analyse] 分析规则类型：一般统计 (KPI_ID:%s, ANA_ID:%s)", m_sKpiID.c_str(), m_sAnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetStatisticsHiveSQL(vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_REPORT_STATISTICS:	// 报表统计
		m_pLog->Output("[Analyse] 分析规则类型：报表统计 (KPI_ID:%s, ANA_ID:%s)", m_sKpiID.c_str(), m_sAnaID.c_str());
		m_pLog->Output("[Analyse] 分析规则表达式：%s", ref_exp.c_str());
		GetReportStatisticsHiveSQL(vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_HIVE_SQL:				// 可执行的HIVE SQL语句
		m_pLog->Output("[Analyse] 分析规则类型：可执行的HIVE SQL语句 (KPI_ID:%s, ANA_ID:%s)", m_sKpiID.c_str(), m_sAnaID.c_str());
		// 分析表达式，即是可执行的Hive SQL语句（可多个，以分号分隔）
		SplitHiveSqlExpress(ref_exp, vec_hivesql);
		break;
	case AnalyseRule::ANATYPE_UNKNOWN:				// 未知类型
	default:
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "不支持的分析规则类型: %d (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.AnaRule.AnaType, m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	// 生成数据库[DB2]信息
	GetAnaDBInfo();
}

// 暂时只支持两组数据的汇总对比
void Analyse::GetSummaryCompareHiveSQL(VEC_STRING& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckDualEtlRule(m_taskInfo.vecEtlRule) )
	{
	case -1:	// 采集规则个数不够
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "只有 %lu 份采集结果数据，无法进行汇总对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecEtlRule.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "采集规则的维度size不一致，无法进行汇总对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "采集规则的值size不一致，无法进行汇总对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 格式样例1: A-B, diff1:diff2:diff3:...:diffn		// 指定 1-n 列
	// 格式样例2: A-B, all								// 所有列
	std::string& ana_exp = m_taskInfo.AnaRule.AnaExpress;

	VEC_STRING vec_str;
	base::PubStr::Str2StrVector(ana_exp, ",", vec_str);
	if ( vec_str.size() != 2 )
	{
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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
			throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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
				throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：不支持的表达式 [%s] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ana_exp.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}

			int diff_no = -1;
			if ( !base::PubStr::Str2Int(ref_str.substr(pos+4), diff_no) )
			{
				throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：[%s] 转换失败! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_str.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}

			if ( diff_no < 1 || diff_no > VAL_SIZE )
			{
				throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：列值 [%d] 越界! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", diff_no, m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}

			if ( set_diff.find(diff_no) != set_diff.end() )
			{
				throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "分析规则表达式解析失败：列值 [%d] 已经存在! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", diff_no, m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}

			set_diff.insert(diff_no);

			// 表达式中DIFF从1开始，而列序号是从0开始的
			vec_col.push_back(diff_no-1);
		}
	}

	VEC_STRING v_hive_sql;

	OneEtlRule& first_one  = m_taskInfo.vecEtlRule[first_index];
	OneEtlRule& second_one = m_taskInfo.vecEtlRule[second_index];

	// 检查：单独显示的字段是否对应
	if ( first_one.vecEtlSingleDim.size() != m_taskInfo.vecLeftKpiCol.size() )
	{
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "汇总对比：左侧，采集的单独显示字段数 [%lu] 与指标的单独显示字段数 [%lu] 不一致！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", first_one.vecEtlSingleDim.size(), m_taskInfo.vecLeftKpiCol.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
	if ( second_one.vecEtlSingleDim.size() != m_taskInfo.vecRightKpiCol.size() )
	{
		throw base::Exception(ANAERR_GET_SUMMARY_FAILED, "汇总对比：右侧，采集的单独显示字段数 [%lu] 与指标的单独显示字段数 [%lu] 不一致！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", second_one.vecEtlSingleDim.size(), m_taskInfo.vecRightKpiCol.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	//// 指定分析条件
	//const std::string CONDITION     = TaskInfoUtil::GetStraightAnaCondition(m_taskInfo.AnaRule.AnaCondType, m_taskInfo.AnaRule.AnaCondition, false);
	//const std::string ADD_CONDITION = TaskInfoUtil::GetStraightAnaCondition(m_taskInfo.AnaRule.AnaCondType, m_taskInfo.AnaRule.AnaCondition, true);

	// 1) 汇总：对平的Hive SQL语句
	std::string hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col, false);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[0] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += " and " + TaskInfoUtil::GetCompareEqualValsByCol(first_one, second_one, vec_col) + ")";
	//TaskInfoUtil::AddConditionSql(hive_sql, CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 2) 汇总：有差异的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col, false);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[1] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetCompareUnequalValsByCol(first_one, second_one, vec_col);
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	//// 3) 汇总：取 A、B 两份汇总数据，（后续）找出非共有的数据
	//hive_sql = TaskInfoUtil::GetOneEtlRuleDetailSQL(first_one);
	//v_hive_sql.push_back(hive_sql);

	//hive_sql = TaskInfoUtil::GetOneEtlRuleDetailSQL(second_one);
	//v_hive_sql.push_back(hive_sql);

	// 3) 汇总：差异“左有右无”的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col, true);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[1] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a left outer join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetOneRuleValsNull(second_one, "b.");
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 4) 汇总：差异“左无右有”的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(second_one, first_one, vec_col, true, true);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[1] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + second_one.TargetPatch + " b left outer join " + first_one.TargetPatch;
	hive_sql += " a on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetOneRuleValsNull(first_one, "a.");
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	v_hive_sql.swap(vec_hivesql);
}

// 暂时只支持两组数据的明细对比
void Analyse::GetDetailCompareHiveSQL(VEC_STRING& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckDualEtlRule(m_taskInfo.vecEtlRule) )
	{
	case -1:	// 采集规则个数不够
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "只有 %lu 份采集结果数据，无法进行明细对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecEtlRule.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "采集规则的维度size不一致，无法进行明细对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "采集规则的值size不一致，无法进行明细对比! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 格式样例: A-B
	// 暂时只支持这一种表达式："A-B"
	int first_index  = -1;
	int second_index = -1;
	DetermineDataGroup(m_taskInfo.AnaRule.AnaExpress, first_index, second_index);

	std::vector<int> vec_col;
	VEC_STRING       v_hive_sql;

	OneEtlRule& first_one  = m_taskInfo.vecEtlRule[first_index];
	OneEtlRule& second_one = m_taskInfo.vecEtlRule[second_index];

	// 检查：单独显示的字段是否对应
	if ( first_one.vecEtlSingleDim.size() != m_taskInfo.vecLeftKpiCol.size() )
	{
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "明细对比：左侧，采集的单独显示字段数 [%lu] 与指标的单独显示字段数 [%lu] 不一致！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", first_one.vecEtlSingleDim.size(), m_taskInfo.vecLeftKpiCol.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
	if ( second_one.vecEtlSingleDim.size() != m_taskInfo.vecRightKpiCol.size() )
	{
		throw base::Exception(ANAERR_GET_DETAIL_FAILED, "明细对比：右侧，采集的单独显示字段数 [%lu] 与指标的单独显示字段数 [%lu] 不一致！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", second_one.vecEtlSingleDim.size(), m_taskInfo.vecRightKpiCol.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	//// 指定分析条件
	//const std::string CONDITION     = TaskInfoUtil::GetStraightAnaCondition(m_taskInfo.AnaRule.AnaCondType, m_taskInfo.AnaRule.AnaCondition, false);
	//const std::string ADD_CONDITION = TaskInfoUtil::GetStraightAnaCondition(m_taskInfo.AnaRule.AnaCondType, m_taskInfo.AnaRule.AnaCondition, true);

	// 1) 明细：对平的Hive SQL语句
	std::string hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col, false);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[0] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += " and " + TaskInfoUtil::GetCompareEqualVals(first_one, second_one) + ")";
	//TaskInfoUtil::AddConditionSql(hive_sql, CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 2) 明细：有差异的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col, false);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[1] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetCompareUnequalVals(first_one, second_one);
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 3) 明细：左有右无的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(first_one, second_one, vec_col, true);
	hive_sql += ", '" + m_taskInfo.vecComResDesc[2] + "'" + TaskInfoUtil::GetBothSingleDims(first_one, second_one);
	hive_sql += " from " + first_one.TargetPatch + " a left outer join " + second_one.TargetPatch;
	hive_sql += " b on (" + TaskInfoUtil::GetCompareDims(first_one, second_one);
	hive_sql += ") where " + TaskInfoUtil::GetOneRuleValsNull(second_one, "b.");
	//TaskInfoUtil::AddConditionSql(hive_sql, ADD_CONDITION);

	v_hive_sql.push_back(hive_sql);

	// 4) 明细：左无右有的Hive SQL语句
	hive_sql = "select " + TaskInfoUtil::GetCompareFieldsByCol(second_one, first_one, vec_col, true, true);
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

void Analyse::GetStatisticsHiveSQL(VEC_STRING& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckPluralEtlRule(m_taskInfo.vecEtlRule) )
	{
	case -1:	// 没有采集规则
		throw base::Exception(ANAERR_GET_STATISTICS_FAILED, "没有采集结果数据，无法进行一般统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecEtlRule.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_STATISTICS_FAILED, "采集规则的维度size不一致，无法进行一般统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_STATISTICS_FAILED, "采集规则的值size不一致，无法进行一般统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 当有多份采集结果数据时，采用联合语句
	bool is_union_all = (m_taskInfo.vecEtlRule.size() > 1);

	if ( m_taskInfo.AnaRule.AnaExpress.empty() )	// 没有指定表达式，则取全量数据
	{
		TaskInfoUtil::GetEtlStatisticsSQLs(m_taskInfo.vecEtlRule, vec_hivesql, is_union_all);
	}
	else
	{
		GetStatisticsHiveSQLBySet(vec_hivesql, is_union_all);
	}

	//AddAnalysisCondition(m_taskInfo.AnaRule, vec_hivesql);
}

void Analyse::GetReportStatisticsHiveSQL(VEC_STRING& vec_hivesql) throw(base::Exception)
{
	switch ( TaskInfoUtil::CheckPluralEtlRule(m_taskInfo.vecEtlRule) )
	{
	case -1:	// 没有采集规则
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "没有采集结果数据，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.vecEtlRule.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	case -2:	// 维度不一致
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "采集规则的维度size不一致，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	case -3:	// 值不一致
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "采集规则的值size不一致，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		break;
	default:	// 正常
		break;
	}

	// 采集规则的值是否唯一
	// 由于上面已经确保了值的size都是一致的，所以检查第一组就可以了
	if ( m_taskInfo.vecEtlRule[0].vecEtlVal.size() != 1 )
	{
		throw base::Exception(ANAERR_GET_REPORT_STAT_FAILED, "采集规则的值不唯一，无法进行报表统计! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	TaskInfoUtil::GetEtlStatisticsSQLs(m_taskInfo.vecEtlRule, vec_hivesql, false);
	//AddAnalysisCondition(m_taskInfo.AnaRule, vec_hivesql);
}

void Analyse::GetStatisticsHiveSQLBySet(VEC_STRING& vec_hivesql, bool union_all) throw(base::Exception)
{
	std::set<int> set_int;
	const int RESULT = base::PubStr::Express2IntSet(m_taskInfo.AnaRule.AnaExpress, set_int);

	if ( -1 == RESULT )		// 转换失败
	{
		throw base::Exception(ANAERR_GET_STAT_BY_SET_FAILED, "分析规则解析失败：分析规则表达式转换失败! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
	else if ( 1 == RESULT )		// 数据重复
	{
		throw base::Exception(ANAERR_GET_STAT_BY_SET_FAILED, "分析规则解析失败：分析规则表达式存在重复数据! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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
				throw base::Exception(ANAERR_GET_STAT_BY_SET_FAILED, "分析规则解析失败：不存在指定的采集组 [%d] ! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", VAL, m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}
		}

		TaskInfoUtil::GetEtlStatisticsSQLsBySet(m_taskInfo.vecEtlRule, set_int, vec_hivesql, union_all);
	}
}

void Analyse::SplitHiveSqlExpress(const std::string& exp_sqls, VEC_STRING& vec_hivesql) throw(base::Exception)
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

	VEC_STRING v_hive_sql;
	base::PubStr::Str2StrVector(exp, ";", v_hive_sql);

	if ( v_hive_sql.empty() )
	{
		throw base::Exception(ANAERR_SPLIT_HIVESQL_FAILED, "拆分失败：没有可执行Hive SQL语句! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	v_hive_sql.swap(vec_hivesql);
}

void Analyse::DetermineDataGroup(const std::string& exp, int& first, int& second) throw(base::Exception)
{
	VEC_STRING vec_str;
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
	const base::PubTime::DATE_TYPE DT = m_dbinfo.GetEtlDateType();
	const std::string DT_DESC         = base::PubTime::DateType2String(DT);
	const std::string ETL_DAY         = m_dbinfo.GetEtlDay();

	ReleaseSQLTranslator();
	m_pSQLTranslator = new base::SQLTranslator(DT, ETL_DAY);
	if ( NULL == m_pSQLTranslator )
	{
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "new SQLTranslator failed: 无法申请到内存空间! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	AnaTaskInfo::ResultTableType& type = m_taskInfo.ResultType;
	std::string tab_name = base::PubStr::TrimB(m_taskInfo.TableName);
	switch ( type )
	{
	case AnaTaskInfo::TABTYPE_COMMON:		// 普通表
		// Do nothing
		m_pLog->Output("[Analyse] 结果表类型：普通表");
		break;
	case AnaTaskInfo::TABTYPE_DAY:			// 天表
		if ( DT > base::PubTime::DT_DAY )
		{
			throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "采集时间类型与结果表类型无法匹配！采集时间类型为：%s，结果表类型为：DAY_TABLE (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", DT_DESC.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}
		else
		{
			tab_name = tab_name + "_" + ETL_DAY;
		}
		m_pLog->Output("[Analyse] 结果表类型：天表");
		break;
	case AnaTaskInfo::TABTYPE_MONTH:		// 月表
		if ( DT > base::PubTime::DT_MONTH )
		{
			throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "采集时间类型与结果表类型无法匹配！采集时间类型为：%s，结果表类型为：MONTH_TABLE (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", DT_DESC.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}
		else
		{
			tab_name = tab_name + "_" + ETL_DAY.substr(0, 6);
		}
		m_pLog->Output("[Analyse] 结果表类型：月表");
		break;
	case AnaTaskInfo::TABTYPE_YEAR:			// 年表
		tab_name = tab_name + "_" + ETL_DAY.substr(0, 4);
		m_pLog->Output("[Analyse] 结果表类型：年表");
		break;
	case AnaTaskInfo::TABTYPE_UNKNOWN:		// 未知类型
	default:
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "无法识别的目标表类型: TABTYPE_UNKNOWN (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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
	// 先进行数据补全
	DataSupplement();

	SrcDataUnifiedCoding();

	const int BEG_POS = m_taskInfo.vecKpiDimCol.size();
	const int END_POS = BEG_POS + m_taskInfo.vecKpiValCol.size();
	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == m_taskInfo.AnaRule.AnaType )		// 报表统计
	{
		m_pLog->Output("[Analyse] 报表统计类型数据转换 ...");

		m_pLog->Output("[Analyse] (转换前) 源数据大小为: %llu", base::PubStr::CalcVVVectorStr(m_v3HiveSrcData));
		GenerateReportStatData();
		m_pLog->Output("[Analyse] (转换后) 报表数据大小为: %llu", m_v2ReportStatData.size());

		// 将源数据中的空值字符串("NULL")转换为("0")
		base::PubStr::ReplaceInStrVector2(m_v2ReportStatData, "NULL", "0", false, true);

		// 将数组中的值进行格式化
		base::PubStr::FormatValueStrVector(m_v2ReportStatData, BEG_POS, END_POS);
	}
	else	// 非报表统计
	{
		//// 生成明细结果数据
		//CompareResultData();

		const int VEC3_SIZE = m_v3HiveSrcData.size();
		for ( int i = 0; i < VEC3_SIZE; ++i )
		{
			VEC2_STRING& ref_vec2 = m_v3HiveSrcData[i];

			// 将源数据中的空值字符串("NULL")转换为("0")
			base::PubStr::ReplaceInStrVector2(ref_vec2, "NULL", "0", false, true);

			// 将数组中的值进行格式化
			base::PubStr::FormatValueStrVector(ref_vec2, BEG_POS, END_POS);
		}
	}

	m_pLog->Output("[Analyse] 分析完成!");
}

void Analyse::SrcDataUnifiedCoding() throw(base::Exception)
{
	const int DIM_REGION_INDEX  = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_REGION);
	const int DIM_CHANNEL_INDEX = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_CHANNEL);

	std::set<std::string> set_not_exist_region;			// 未成功转换的地市别名
	std::set<std::string> set_not_exist_channel;		// 未成功转换的渠道别名

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		VEC2_STRING& ref_vec2 = m_v3HiveSrcData[i];

		const size_t VEC2_SIZE = ref_vec2.size();
		for ( size_t j = 0; j < VEC2_SIZE; ++j )
		{
			VEC_STRING& ref_vec1 = ref_vec2[j];

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

void Analyse::GenerateReportStatData()
{
	YDReportInput  input(m_v3HiveSrcData);
	YDReportOutput output(m_v2ReportStatData);

	// 将 HIVE 源数据转换为报表统计类型数据
	ReportConverter rc;
	rc.ReportStatDataConvertion(&input, &output);

	// 释放源数据占用的资源
	input.ReleaseSourceData();
}

//void Analyse::CompareResultData() throw(base::Exception)
//{
//	// 是否为对比分析类型？
//	if ( AnalyseRule::ANATYPE_DETAIL_COMPARE == m_taskInfo.AnaRule.AnaType 
//		|| AnalyseRule::ANATYPE_SUMMARY_COMPARE == m_taskInfo.AnaRule.AnaType )
//	{
//		if ( m_v3HiveSrcData.size() != COMPARE_HIVE_SRCDATA_SIZE )
//		{
//			throw base::Exception(ANAERR_COMPARE_RESULT_DATA, "不正确的对比源数据个数: %lu (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_v3HiveSrcData.size(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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
//		const int DIM_SIZE = ;
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
//		VEC2_STRING vec2_result;
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

void Analyse::DataSupplement()
{
	int first_index  = 0;
	int second_index = 0;

	// 没有需要补全的时间字段
	if ( !m_dbinfo.GetEtlDayIndex(first_index) && !m_dbinfo.GetNowDayIndex(second_index) )
	{
		m_pLog->Output("[Analyse] 无需进行数据补全！");
		return;
	}
	m_pLog->Output("[Analyse] 进行时间数据补全 ...");

	std::string first_time;
	std::string second_time;

	if ( first_index < second_index )	// 采集时间在前，当前时间在后
	{
		first_time  = m_dbinfo.GetEtlDay();
		second_time = m_dbinfo.GetNowDay();
	}
	else	// 当前时间在前，采集时间在后
	{
		// 交换索引位置
		int temp     = first_index;
		first_index  = second_index;
		second_index = temp;

		first_time  = m_dbinfo.GetNowDay();
		second_time = m_dbinfo.GetEtlDay();
	}

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		VEC2_STRING& ref_vec2 = m_v3HiveSrcData[i];

		const size_t VEC2_SIZE = ref_vec2.size();
		for ( size_t s = 0; s < VEC2_SIZE; ++s )
		{
			VEC_STRING& ref_vec = ref_vec2[s];

			if ( first_index != TimeField::TF_INVALID_INDEX )
			{
				ref_vec.insert(ref_vec.begin()+first_index, first_time);
			}

			if ( second_index != TimeField::TF_INVALID_INDEX )
			{
				ref_vec.insert(ref_vec.begin()+second_index, second_time);
			}
		}
	}

	m_pLog->Output("[Analyse] 时间数据补全完成！");
}

void Analyse::CollectDimVal()
{
	m_pLog->Output("[Analyse] 收集源数据的所有维度取值 ...");

	DimVal dv;
	dv.KpiID = m_sKpiID;

	const int VEC_SIZE = m_taskInfo.vecKpiDimCol.size();
	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		VEC2_STRING& ref_vec2 = m_v3HiveSrcData[i];

		const size_t VEC2_SIZE = ref_vec2.size();
		for ( size_t j = 0; j < VEC2_SIZE; ++j )
		{
			VEC_STRING& ref_vec1 = ref_vec2[j];

			for ( int k = 0; k < VEC_SIZE; ++k )
			{
				KpiColumn& ref_col = m_taskInfo.vecKpiDimCol[k];

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
			m_pLog->Output("[Analyse] 准备入库第 %d 组结果数据 ...", (i+1));
			m_pAnaDB2->InsertResultData(m_dbinfo, m_v3HiveSrcData[i]);
		}
	}

	m_pLog->Output("[Analyse] 结果数据存储完毕!");
}

void Analyse::RemoveOldResult(const AnaTaskInfo::ResultTableType& result_tabtype) throw(base::Exception)
{
	// 是否带时间戳
	// 只有带时间戳才可以按采集时间删除结果数据
	int index = 0;
	if ( m_dbinfo.GetEtlDayIndex(index) )
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

