#include "acquire.h"
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "log.h"
#include "pubtime.h"
#include "autodisconnect.h"
#include "cacqdb2.h"
#include "chivethrift.h"


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
	return ("Acquire: Version 1.01.0035 released. Compiled at "__TIME__" on "__DATE__);
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

	std::vector<std::string> vec_fields;
	TaskInfo2TargetFields(info, vec_fields);

	std::vector<std::string> vec_hivesql;
	TaskInfo2HiveSql(info, vec_hivesql);

	m_pLog->Output("[Acquire] 重建采集目标表 ...");
	m_pCHive->RebuildHiveTable(info.EtlRuleTarget, vec_fields);

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
	std::vector<std::string> v_sql;

	std::string hive_sql;
	std::string dim_sql;
	const size_t SRC_SIZE = info.vecEtlRuleDataSrc.size();
	for ( size_t i = 0; i < SRC_SIZE; ++i )
	{
		hive_sql = "insert into table " + info.EtlRuleTarget + " select ";

		dim_sql.clear();
		AcqEtlDim& dim = info.vecEtlRuleDim[i];
		size_t v_size = dim.vecEtlDim.size();
		for ( size_t j = 0; j < v_size; ++j )
		{
			OneEtlDim& one = dim.vecEtlDim[j];

			if ( one.EtlDimSrcName.empty() )
			{
				throw base::Exception(ACQERR_TASKINFO_INVALID, "第 [%lu] 个采集维度的源字段名没有设定! [DIM_ID:%s, DIM_SEQ:%d] [FILE:%s, LINE:%d]", (j+1), one.EtlDimID.c_str(), one.EtlDimSeq, __FILE__, __LINE__);
			}

			if ( j != 0 )
			{
				dim_sql += ", " + one.EtlDimSrcName;
			}
			else
			{
				dim_sql = one.EtlDimSrcName;
			}
		}

		hive_sql += dim_sql;

		AcqEtlVal& val = info.vecEtlRuleVal[i];
		v_size = val.vecEtlVal.size();
		for ( size_t j = 0; j < v_size; ++j )
		{
			OneEtlVal& one = val.vecEtlVal[j];

			if ( one.EtlValSrcName.empty() )
			{
				throw base::Exception(ACQERR_TASKINFO_INVALID, "第 [%lu] 个采集值的源字段名没有设定! [VAL_ID:%s, VAL_SEQ:%d] [FILE:%s, LINE:%d]", (j+1), one.EtlValID.c_str(), one.EtlValSeq, __FILE__, __LINE__);
			}

			hive_sql += ", " + TransEtlValSrcName(one.EtlValSrcName);
		}

		hive_sql += " from " + TransDataSrcDate(info.EtlRuleTime, info.vecEtlRuleDataSrc[i]);
		hive_sql += " group by " + dim_sql;

		v_sql.push_back(hive_sql);
	}

	v_sql.swap(vec_sql);
}

std::string Acquire::TransEtlValSrcName(const std::string& val_srcname)
{
	std::string val = val_srcname;

	boost::trim(val);
	boost::to_upper(val);

	if ( "<RECORD>" == val )	// 记录数
	{
		val = "count(*)";
	}
	else
	{
		val = "sum(" + val + ")";
	}

	return val;
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

