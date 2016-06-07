#include "acquire.h"
#include <vector>
#include "log.h"
#include "pubtime.h"
#include "pubstr.h"
#include "autodisconnect.h"
#include "simpletime.h"
#include "cacqdb2.h"
#include "cacqhive.h"
#include "taskinfoutil.h"


Acquire g_Acquire;


Acquire::Acquire()
:m_nHivePort(0)
,m_nHdfsPort(0)
,m_pAcqDB2(NULL)
,m_pAcqHive(NULL)
{
	g_pApp = &g_Acquire;
}

Acquire::~Acquire()
{
}

const char* Acquire::Version()
{
	return ("Acquire: Version 1.09.0081 released. Compiled at "__TIME__" on "__DATE__);
}

void Acquire::LoadConfig() throw(base::Exception)
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");
	m_cfg.RegisterItem("HIVE_SERVER", "IP_ADDRESS");
	m_cfg.RegisterItem("HIVE_SERVER", "PORT");
	m_cfg.RegisterItem("HIVE_SERVER", "USERNAME");
	m_cfg.RegisterItem("HIVE_SERVER", "PASSWORD");
	m_cfg.RegisterItem("HIVE_SERVER", "LOAD_JAR_PATH");

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
	m_sHiveUsr = m_cfg.GetCfgValue("HIVE_SERVER", "USERNAME");
	m_sHivePwd = m_cfg.GetCfgValue("HIVE_SERVER", "PASSWORD");
	m_sLoadJarPath = m_cfg.GetCfgValue("HIVE_SERVER", "LOAD_JAR_PATH");

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

	m_pHive = new CAcqHive(m_sHiveIP, m_nHivePort, m_sHiveUsr, m_sHivePwd);
	if ( NULL == m_pHive )
	{
		throw base::Exception(ACQERR_INIT_FAILED, "new CAcqHive failed: 无法申请到内存空间!");
	}
	m_pAcqHive = dynamic_cast<CAcqHive*>(m_pHive);

	m_pAcqHive->Init(m_sLoadJarPath);

	m_pLog->Output("Init OK.");
}

void Acquire::Run() throw(base::Exception)
{
	base::AutoDisconnect a_disconn(new base::HiveDB2Connector(m_pAcqDB2, m_pAcqHive));
	a_disconn.Connect();

	AcqTaskInfo task_info;
	SetTaskInfo(task_info);

	FetchTaskInfo(task_info);

	DoDataAcquisition(task_info);
}

void Acquire::GetParameterTaskInfo(const std::string& para) throw(base::Exception)
{
	// 格式：启动批号:指标ID:采集规则ID:...
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(para, ":", vec_str);

	if ( vec_str.size() < 3 )
	{
		throw base::Exception(ACQERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法按格式拆分! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	m_sKpiID = vec_str[1];
	base::PubStr::Trim(m_sKpiID);
	if ( m_sKpiID.empty() )
	{
		throw base::Exception(ACQERR_KPIID_INVALID, "指标ID无效! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_sEtlID = vec_str[2];
	base::PubStr::Trim(m_sEtlID);
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

	switch ( info.DataSrcType )
	{
	case AcqTaskInfo::DSTYPE_HIVE:
		m_pLog->Output("[Acquire] 数据源类型：HIVE");
		HiveDataAcquisition(info);
		break;
	case AcqTaskInfo::DSTYPE_DB2:
		m_pLog->Output("[Acquire] 数据源类型：DB2");
		DB2DataAcquisition(info);
		break;
	case AcqTaskInfo::DSTYPE_UNKNOWN:
	default:
		throw base::Exception(ACQERR_DATA_ACQ_FAILED, "未知的数据源类型: DSTYPE_UNKNOWN [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", info.KpiID.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] 采集数据完成.");
}

void Acquire::HiveDataAcquisition(AcqTaskInfo& info) throw(base::Exception)
{
	RebuildHiveTable(info);

	std::vector<std::string> vec_hivesql;
	TaskInfo2Sql(info, vec_hivesql, true);

	m_pLog->Output("[Acquire] [HIVE] 执行数据采集 ...");
	m_pAcqHive->ExecuteAcqSQL(vec_hivesql);
}

void Acquire::DB2DataAcquisition(AcqTaskInfo& info) throw(base::Exception)
{
	LoadHdfsConfig();

	HdfsConnector* pHdfsConnector = new HdfsConnector(m_sHdfsHost, m_nHdfsPort);

	base::AutoDisconnect a_disconn(pHdfsConnector);		// 资源自动释放
	a_disconn.Connect();

	int field_size = RebuildHiveTable(info);

	std::vector<std::string> vec_sql;
	TaskInfo2Sql(info, vec_sql, false);

	m_pLog->Output("[Acquire] [DB2] 执行数据采集 ...");

	hdfsFS hd_fs = pHdfsConnector->GetHdfsFS();
	const int VEC_SIZE = vec_sql.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		std::vector<std::vector<std::string> > vec2_data;
		m_pAcqDB2->FetchEtlData(vec_sql[i], field_size, vec2_data);

		std::string hdfsFile = GeneralHdfsFileName();
		hdfsFile = DB2DataOutputHdfsFile(vec2_data, hd_fs, hdfsFile);

		LoadHdfsFile2Hive(info.EtlRuleTarget, hdfsFile);
	}
}

void Acquire::LoadHdfsConfig() throw(base::Exception)
{
	m_cfg.RegisterItem("HDFS", "HOST");
	m_cfg.RegisterItem("HDFS", "TMP_PATH");
	m_cfg.RegisterItem("HDFS", "PORT");

	m_cfg.ReadConfig();

	// HDFS配置
	m_sHdfsHost    = m_cfg.GetCfgValue("HDFS", "HOST");
	m_sHdfsTmpPath = m_cfg.GetCfgValue("HDFS", "TMP_PATH");
	m_nHdfsPort    = (int)m_cfg.GetCfgLongVal("HDFS", "PORT");
	if ( m_nHdfsPort <= 0 )
	{
		throw base::Exception(ACQERR_HDFS_PORT_INVALID, "HDFS端口无效! (port=%d) [FILE:%s, LINE:%d]", m_nHdfsPort, __FILE__, __LINE__);
	}

	// 加上末尾的斜杠
	if ( m_sHdfsTmpPath[m_sHdfsTmpPath.size()-1] != '/' )
	{
		m_sHdfsTmpPath += "/";
	}
}

std::string Acquire::GeneralHdfsFileName()
{
	return (m_sKpiID + "_" + m_sEtlID + "_" + base::SimpleTime::Now().Time14());
}

std::string Acquire::DB2DataOutputHdfsFile(std::vector<std::vector<std::string> >& vec2_data, hdfsFS& hd_fs, const std::string& hdfs_file) throw(base::Exception)
{
	const std::string FULL_FILE_PATH = m_sHdfsTmpPath + hdfs_file;

	hdfsFile hd_file = hdfsOpenFile(hd_fs, FULL_FILE_PATH.c_str(), O_WRONLY|O_CREAT, 0, 0, 0);
	if ( NULL == hd_file )
	{
		throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Open file failed: %s [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Acquire] [HDFS] Open file [%s] OK.", FULL_FILE_PATH.c_str());

	m_pLog->Output("[Acquire] [HDFS] Write file [%s] ...", FULL_FILE_PATH.c_str());

	std::string str_buf;
	const size_t VEC2_SIZE = vec2_data.size();
	for ( size_t i = 0; i < VEC2_SIZE; ++i )
	{
		std::vector<std::string>& ref_vec1 = vec2_data[i];

		str_buf.clear();

		const int V1_SIZE = ref_vec1.size();
		for ( int j = 0; j < V1_SIZE; ++j )
		{
			if ( j != 0 )
			{
				str_buf += "|" + ref_vec1[j];
			}
			else
			{
				str_buf += ref_vec1[j];
			}
		}

		// 不是最后一行，则加上换行
		if ( i < VEC2_SIZE - 1 )
		{
			str_buf += "\n";
		}

		if ( hdfsWrite(hd_fs, hd_file, (void*)str_buf.c_str(), str_buf.size()) < 0 )
		{
			throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Write file failed: %s (index:%llu) [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), i, __FILE__, __LINE__);
		}

		// 每一千行，flush一次buffer
		if ( i % 1000 == 0 && i != 0 )
		{
			if ( hdfsFlush(hd_fs, hd_file) < 0 )
			{
				throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Flush file failed: %s (index:%llu) [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), i, __FILE__, __LINE__);
			}
		}
	}

	// 最后再flush一次
	if ( hdfsFlush(hd_fs, hd_file) < 0 )
	{
		throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Finally flush file failed: %s [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] [HDFS] Write file line(s): %llu", VEC2_SIZE);

	if ( hdfsCloseFile(hd_fs, hd_file) < 0 )
	{
		throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Close file failed: %s [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Acquire] [HDFS] Close file [%s].", FULL_FILE_PATH.c_str());

	return FULL_FILE_PATH;
}

void Acquire::LoadHdfsFile2Hive(const std::string& target_tab, const std::string& hdfs_file) throw(base::Exception)
{
	if ( hdfs_file.empty() )
	{
		throw base::Exception(ACQERR_LOAD_HIVE_FAILED, "[Acquire] HDFS文件名无效！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] Load HDFS file [%s] to HIVE ...", hdfs_file.c_str());

	std::string load_sql;
	base::PubStr::SetFormatString(load_sql, "load data inpath 'hdfs://%s:%d", m_sHdfsHost.c_str(), m_nHdfsPort);

	if ( hdfs_file[0] != '/' )
	{
		load_sql += "/";
	}

	load_sql += hdfs_file + "' into table " + target_tab;

	std::vector<std::string> vec_sql;
	vec_sql.push_back(load_sql);
	m_pAcqHive->ExecuteAcqSQL(vec_sql);

	m_pLog->Output("[Acquire] Load HDFS file to HIVE ---- done!");
}

int Acquire::RebuildHiveTable(AcqTaskInfo& info) throw(base::Exception)
{
	m_pLog->Output("[Acquire] 重建 HIVE 采集目标表 ...");

	std::vector<std::string> vec_field;
	TaskInfo2TargetFields(info, vec_field);

	m_pAcqHive->RebuildTable(info.EtlRuleTarget, vec_field);

	m_pLog->Output("[Acquire] HIVE 采集目标表 [%s] 重建完成!", info.EtlRuleTarget.c_str());
	return vec_field.size();
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

void Acquire::TaskInfo2Sql(AcqTaskInfo& info, std::vector<std::string>& vec_sql, bool hive) throw(base::Exception)
{
	switch ( info.EtlCondType )
	{
	case AcqTaskInfo::ETLCTYPE_NONE:		// 不带条件
	case AcqTaskInfo::ETLCTYPE_STRAIGHT:	// 直接条件
		NoneOrStraight2Sql(info, vec_sql, hive);
		break;
	case AcqTaskInfo::ETLCTYPE_OUTER_JOIN:	// 外连条件
		OuterJoin2Sql(info, vec_sql, hive);
		break;
	case AcqTaskInfo::ETLCTYPE_UNKNOWN:		// 未知类型
	default:
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集规则解析失败：未知的采集条件类型! (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
}

void Acquire::OuterJoin2Sql(AcqTaskInfo& info, std::vector<std::string>& vec_sql, bool hive) throw(base::Exception)
{
	// 分析采集条件
	// 格式：[外连表名]:[关联的维度字段(逗号分隔)]
	std::string& etl_cond = info.EtlCondition;
	base::PubStr::Trim(etl_cond);

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(etl_cond, ":", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：无法识别的采集条件 [%s] (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", etl_cond.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	std::string outer_table = vec_str[0];		// 外连表名
	if ( outer_table.empty() )
	{
		throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：无法识别的采集条件 [%s] (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", etl_cond.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	// 外连表名也可通过采集周期来指定
	outer_table = TransDataSrcDate(info.EtlRuleTime, outer_table);

	// 拆分关联字段
	const std::string OUTER_ON = vec_str[1];
	base::PubStr::Str2StrVector(OUTER_ON, ",", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：无法识别的采集条件 [%s] (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", etl_cond.c_str(), info.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	std::string sql;
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

		if ( hive )
		{
			sql += "insert into table " + info.EtlRuleTarget + " ";
		}

		sql += TaskInfoUtil::GetOuterJoinEtlSQL(info.vecEtlRuleDim[0], info.vecEtlRuleVal[0], SRC_TABLE, outer_table, vec_str);

		v_sql.push_back(sql);
	}
	else	// 多个源数据
	{
		std::string target_dim_sql = TaskInfoUtil::GetTargetDimSql(info.vecEtlRuleDim[0]);

		// Hive SQL head
		if ( hive )
		{
			sql += "insert into table " + info.EtlRuleTarget + " ";
		}

		sql += "select ";
		sql += target_dim_sql + TaskInfoUtil::GetTargetValSql(info.vecEtlRuleVal[0]);
		sql += " from (";

		int num_join_on = 0;
		std::string src_table;

		// Hive SQL body
		for ( int i = 0; i < SRC_SIZE; ++i )
		{
			if ( i != 0 )
			{
				sql += " union all ";
			}

			AcqEtlDim& ref_etl_dim = info.vecEtlRuleDim[i];

			num_join_on = TaskInfoUtil::GetNumOfEtlDimJoinOn(ref_etl_dim);
			if ( OUTER_ON_SIZE != num_join_on )
			{
				throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：采集条件的关联字段数 [%d] 与 维度规则的关联字段数 [%d] (ETLRULE_ID:%s, ETLDIM_ID:%s) [FILE:%s, LINE:%d]", OUTER_ON_SIZE, num_join_on, info.EtlRuleID.c_str(), ref_etl_dim.acqEtlDimID.c_str(), __FILE__, __LINE__);
			}

			src_table = TransDataSrcDate(info.EtlRuleTime, info.vecEtlRuleDataSrc[i]);

			AcqEtlVal& ref_etl_val = info.vecEtlRuleVal[i];

			sql += TaskInfoUtil::GetOuterJoinEtlSQL(ref_etl_dim, ref_etl_val, src_table, outer_table, vec_str);
		}

		// Hive SQL tail
		sql += ") TMP group by " + target_dim_sql;

		v_sql.push_back(sql);
	}

	v_sql.swap(vec_sql);
}

void Acquire::NoneOrStraight2Sql(AcqTaskInfo& info, std::vector<std::string>& vec_sql, bool hive)
{
	std::string condition;
	// 是否为直接条件
	if ( AcqTaskInfo::ETLCTYPE_STRAIGHT == info.EtlCondType )
	{
		condition = info.EtlCondition;
		base::PubStr::Trim(condition);

		std::string head_where = condition.substr(0, 5);
		base::PubStr::Upper(head_where);

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

	std::string sql;
	std::vector<std::string> v_sql;

	const int SRC_SIZE = info.vecEtlRuleDataSrc.size();
	if ( 1 == SRC_SIZE )	// 单个源数据
	{
		if ( hive )
		{
			sql += "insert into table " + info.EtlRuleTarget + " ";
		}

		sql += "select ";
		sql += TaskInfoUtil::GetEtlDimSql(info.vecEtlRuleDim[0], true) + TaskInfoUtil::GetEtlValSql(info.vecEtlRuleVal[0]);
		sql += " from " + TransDataSrcDate(info.EtlRuleTime, info.vecEtlRuleDataSrc[0]) + condition;
		sql += " group by " + TaskInfoUtil::GetEtlDimSql(info.vecEtlRuleDim[0], false);

		v_sql.push_back(sql);
	}
	else	// 多个源数据
	{
		const std::string TARGET_DIM_SQL = TaskInfoUtil::GetTargetDimSql(info.vecEtlRuleDim[0]);

		// SQL head
		if ( hive )
		{
			sql += "insert into table " + info.EtlRuleTarget + " ";
		}

		sql += "select ";
		sql += TARGET_DIM_SQL + TaskInfoUtil::GetTargetValSql(info.vecEtlRuleVal[0]);
		sql += " from (";

		// SQL body
		std::string tab_alias;
		std::string tab_pre;
		for ( int i = 0; i < SRC_SIZE; ++i )
		{
			if ( i != 0 )
			{
				sql += " union all select ";
			}
			else
			{
				sql += "select ";
			}

			tab_alias = base::PubStr::TabIndex2TabAlias(i);
			tab_pre = tab_alias + ".";
			AcqEtlDim& dim = info.vecEtlRuleDim[i];
			AcqEtlVal& val = info.vecEtlRuleVal[i];

			sql += TaskInfoUtil::GetEtlDimSql(dim, true, tab_pre) + TaskInfoUtil::GetEtlValSql(val, tab_pre);
			sql += " from " + TransDataSrcDate(info.EtlRuleTime, info.vecEtlRuleDataSrc[i]) + " " + tab_alias;
			sql += " group by " + TaskInfoUtil::GetEtlDimSql(dim, false, tab_pre);
		}

		// SQL tail
		sql += ") TMP " + condition + " group by " + TARGET_DIM_SQL;

		v_sql.push_back(sql);
	}

	v_sql.swap(vec_sql);
}

std::string Acquire::TransDataSrcDate(const std::string& time, const std::string& data_src) throw(base::Exception)
{
	std::string date;
	base::PubTime::DATE_TYPE d_type;
	if ( !base::PubTime::DateApartFromNow(time, d_type, date) )
	{
		throw base::Exception(ACQERR_TRANS_DATASRC_FAILED, "采集时间转换失败！无法识别的采集时间表达式：%s [FILE:%s, LINE:%d]", time.c_str(), __FILE__, __LINE__);
	}

	std::string be_replace;
	if ( base::PubTime::DT_DAY == d_type )		// 日类型
	{
		be_replace = "YYYYMMDD";
	}
	else if ( base::PubTime::DT_MONTH == d_type )	// 月类型
	{
		be_replace = "YYYYMM";
	}
	else
	{
		throw base::Exception(ACQERR_TRANS_DATASRC_FAILED, "未知的采集时间类型！无法识别的采集时间表达式：%s [FILE:%s, LINE:%d]", time.c_str(), __FILE__, __LINE__);
	}

	// 找到时间标记才进行替换
	std::string new_datasrc = base::PubStr::TrimUpperB(data_src);
	size_t t_pos = new_datasrc.find(be_replace);
	if ( t_pos != std::string::npos )
	{
		new_datasrc.replace(t_pos, be_replace.size(), date);
	}

	return new_datasrc;
}

