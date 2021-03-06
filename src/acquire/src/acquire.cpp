#include "acquire.h"
#include <errno.h>
#include <string.h>
#include <vector>
#include "log.h"
#include "pubstr.h"
#include "basedir.h"
#include "autodisconnect.h"
#include "simpletime.h"
#include "cacqdb2.h"
#include "cacqhive.h"
#include "taskinfoutil.h"
#include "sqltranslator.h"


Acquire::Acquire()
:m_bRetainResult(false)
,m_nHdfsPort(0)
,m_seqHdfsFile(0)
,m_pAcqDB2(NULL)
,m_pAcqHive(NULL)
,m_acqDateType(base::PubTime::DT_UNKNOWN)
,m_pSQLTrans(NULL)
{
}

Acquire::~Acquire()
{
	SQLTransRelease();
}

const char* Acquire::Version()
{
	return ("Acquire: Version 7.0.0 released. Compiled at " __TIME__ " on " __DATE__ ", by G++-" __VERSION__);
}

void Acquire::LoadConfig()
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("SYS", "JVM_INIT_MEM_SIZE");
	m_cfg.RegisterItem("SYS", "JVM_MAX_MEM_SIZE");
	m_cfg.RegisterItem("SYS", "RETAIN_RESULT");
	m_cfg.RegisterItem("SYS", "OVER_WRITE");
	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");
	m_cfg.RegisterItem("HIVE_SERVER", "ZK_QUORUM");
	m_cfg.RegisterItem("HIVE_SERVER", "KRB5_CONF");
	m_cfg.RegisterItem("HIVE_SERVER", "USR_KEYTAB");
	m_cfg.RegisterItem("HIVE_SERVER", "PRINCIPAL");
	m_cfg.RegisterItem("HIVE_SERVER", "JAAS_CONF");
	m_cfg.RegisterItem("HIVE_SERVER", "LOAD_JAR_PATH");

	// 测试环境：不指定建表的 location
	if ( !m_isTest )
	{
		m_cfg.RegisterItem("HIVE_SERVER", "HIVE_TAB_LOCATION");
	}

	m_cfg.RegisterItem("TABLE", "TAB_KPI_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_DIM");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_VAL");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_SRC");

	m_cfg.ReadConfig();

	// 设置 Java 虚拟机初始化内存大小 (MB)
	base::BaseJHive::SetJVMInitMemSize(m_cfg.GetCfgUIntVal("SYS", "JVM_INIT_MEM_SIZE"));
	// 设置 Java 虚拟机最大内存大小 (MB)
	base::BaseJHive::SetJVMMaxMemSize(m_cfg.GetCfgUIntVal("SYS", "JVM_MAX_MEM_SIZE"));

	// 是否保留结果数据
	m_bRetainResult = m_cfg.GetCfgBoolVal("SYS", "RETAIN_RESULT");
	// 是否覆盖重复的结果数据
	m_sInsertMode = (m_cfg.GetCfgBoolVal("SYS", "OVER_WRITE") ? "overwrite" : "into");

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

	// 测试环境：不指定建表的 location
	if ( !m_isTest )
	{
		m_hiveTabLocation = m_cfg.GetCfgValue("HIVE_SERVER", "HIVE_TAB_LOCATION");
		base::BaseDir::DirWithSlash(m_hiveTabLocation);
	}

	// Tables
	m_tabKpiRule = m_cfg.GetCfgValue("TABLE", "TAB_KPI_RULE");
	m_tabEtlRule = m_cfg.GetCfgValue("TABLE", "TAB_ETL_RULE");
	m_tabEtlDim  = m_cfg.GetCfgValue("TABLE", "TAB_ETL_DIM");
	m_tabEtlVal  = m_cfg.GetCfgValue("TABLE", "TAB_ETL_VAL");
	m_tabEtlSrc  = m_cfg.GetCfgValue("TABLE", "TAB_ETL_SRC");

	m_pLog->Output("[Acquire] Load configuration OK.");
}

void Acquire::Init()
{
	GetParameterTaskInfo(GetTaskParaInfo());

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
	m_pAcqDB2->SetTabEtlSrc(m_tabEtlSrc);

	// 连接数据库
	m_pAcqDB2->Connect();

	if ( m_isTest )		// 测试环境
	{
		m_pHive = new CAcqHive(base::BaseJHive::S_DEBUG_HIVE_JAVA_CLASS_NAME);
	}
	else	// 非测试环境
	{
		m_pHive = new CAcqHive(base::BaseJHive::S_RELEASE_HIVE_JAVA_CLASS_NAME);
	}

	if ( NULL == m_pHive )
	{
		throw base::Exception(ACQERR_INIT_FAILED, "new CAcqHive failed: 无法申请到内存空间!");
	}
	m_pAcqHive = dynamic_cast<CAcqHive*>(m_pHive);

	m_pAcqHive->Init(m_sLoadJarPath);
	m_pAcqHive->SetZooKeeperQuorum(m_zk_quorum);
	m_pAcqHive->SetKrb5Conf(m_krb5_conf);
	m_pAcqHive->SetUserKeytab(m_usr_keytab);
	m_pAcqHive->SetPrincipal(m_principal);
	m_pAcqHive->SetJaasConf(m_jaas_conf);

	m_pLog->Output("[Acquire] Init OK.");
}

void Acquire::Run()
{
	base::AutoDisconnect a_disconn(new base::HiveConnector(m_pAcqHive));
	a_disconn.Connect();

	SetTaskInfo();

	FetchTaskInfo();

	DoDataAcquisition();
}

void Acquire::End(int err_code, const std::string& err_msg /*= std::string()*/)
{
	// 断开数据库连接
	if ( m_pAcqDB2 != NULL )
	{
		m_pAcqDB2->Disconnect();
	}
}

void Acquire::SQLTransRelease()
{
	if ( m_pSQLTrans != NULL )
	{
		delete m_pSQLTrans;
		m_pSQLTrans = NULL;
	}
}

void Acquire::GetParameterTaskInfo(const std::string& para)
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

	GetExtendParaTaskInfo(vec_str);
}

void Acquire::SetTaskInfo()
{
	m_taskInfo.KpiID     = m_sKpiID;
	m_taskInfo.EtlRuleID = m_sEtlID;
}

void Acquire::FetchTaskInfo()
{
	m_pLog->Output("[Acquire] 查询采集任务规则信息 ...");
	m_pAcqDB2->SelectEtlTaskInfo(m_taskInfo);

	m_pLog->Output("[Acquire] 检查采集任务规则信息 ...");
	CheckTaskInfo();
}

void Acquire::CheckTaskInfo()
{
	if ( m_taskInfo.EtlRuleTime.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集时间（周期）为空! 无效! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	if ( m_taskInfo.EtlRuleTarget.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集目标表为空! 无效! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	if ( m_taskInfo.vecEtlRuleDataSrc.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "没有采集数据源! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	// 源表与之对应的采集维度、采集值必须一致！
	// 注：后续的操作，这三个vector都会共用vecEtlRuleDataSrc的下标
	size_t src_size = m_taskInfo.vecEtlRuleDataSrc.size();
	size_t dim_size = m_taskInfo.vecEtlRuleDim.size();
	size_t val_size = m_taskInfo.vecEtlRuleVal.size();

	if ( src_size != dim_size )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集数据源个数 (src_size:%lu) 与采集维度个数 (dim_size:%lu) 不一致! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", src_size, dim_size, m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	if ( src_size != val_size )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集数据源个数 (src_size:%lu) 与采集值个数 (val_size:%lu) 不一致! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", src_size, val_size, m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	for ( size_t i = 0; i < dim_size; ++i )
	{
		AcqEtlDim& dim = m_taskInfo.vecEtlRuleDim[i];
		if ( dim.vecEtlDim.empty() )
		{
			throw base::Exception(ACQERR_TASKINFO_INVALID, "采集维度为空! 无效! [DIM_ID:%s] [FILE:%s, LINE:%d]", dim.acqEtlDimID.c_str(), __FILE__, __LINE__);
		}

		AcqEtlVal& val = m_taskInfo.vecEtlRuleVal[i];
		if ( val.vecEtlVal.empty() )
		{
			throw base::Exception(ACQERR_TASKINFO_INVALID, "采集值为空! 无效! [VAL_ID:%s] [FILE:%s, LINE:%d]", val.acqEtlValID.c_str(), __FILE__, __LINE__);
		}
	}

	// 未知的采集条件类型
	if ( AcqTaskInfo::ETLCTYPE_UNKNOWN == m_taskInfo.EtlCondType )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "未知的采集条件类型: ETLCTYPE_UNKNOWN [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] 采集类型：[%s] (%s)", m_sType.c_str(), (m_isTest ? "测试":"发布"));
}

void Acquire::DoDataAcquisition()
{
	m_pLog->Output("[Acquire] 分析采集规则 ...");

	GenerateEtlDate(m_taskInfo.EtlRuleTime);

	switch ( m_taskInfo.DataSrcType )
	{
	case AcqTaskInfo::DSTYPE_HIVE:
		m_pLog->Output("[Acquire] 数据源类型：HIVE");
		HiveDataAcquisition();
		break;
	case AcqTaskInfo::DSTYPE_DB2:
		m_pLog->Output("[Acquire] 数据源类型：DB2");
		DB2DataAcquisition();
		break;
	case AcqTaskInfo::DSTYPE_UNKNOWN:
	default:
		throw base::Exception(ACQERR_DATA_ACQ_FAILED, "未知的数据源类型: DSTYPE_UNKNOWN [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] 采集数据完成.");
}

void Acquire::GenerateEtlDate(const std::string& date_fmt)
{
	if ( !base::PubTime::DateApartFromNow(date_fmt, m_acqDateType, m_acqDate) )
	{
		throw base::Exception(ACQERR_GEN_ETL_DATE_FAILED, "采集时间转换失败！无法识别的采集时间表达式：%s [FILE:%s, LINE:%d]", date_fmt.c_str(), __FILE__, __LINE__);
	}

	SQLTransRelease();
	m_pSQLTrans = new base::SQLTranslator(m_acqDateType, m_acqDate);
	if ( NULL == m_pSQLTrans )
	{
		throw base::Exception(ACQERR_GEN_ETL_DATE_FAILED, "new SQLTranslator failed: 无法申请到内存空间! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] 完成采集时间转换：[%s] -> [%s]", date_fmt.c_str(), m_acqDate.c_str());
}

void Acquire::HiveDataAcquisition()
{
	RebuildHiveTable();

	// 重建目标表后，再检查源表是否存在
	CheckSourceTable(true);

	std::vector<std::string> vec_hivesql;
	TaskInfo2Sql(vec_hivesql, true);

	m_pLog->Output("[Acquire] 执行 HIVE 的数据采集 ...");
	m_pAcqHive->ExecuteAcqSQL(vec_hivesql);
}

void Acquire::DB2DataAcquisition()
{
	// 载入 HDFS 配置
	LoadHdfsConfig();

	// 创建 HDFS 连接
	HdfsConnector* pHdfsConnector = new HdfsConnector(m_sHdfsHost, m_nHdfsPort);
	base::AutoDisconnect a_disconn(pHdfsConnector);		// 资源自动释放
	a_disconn.Connect();

	RebuildHiveTable();

	// 重建目标表后，再检查源表是否存在
	CheckSourceTable(false);

	std::vector<std::string> vec_sql;
	TaskInfo2Sql(vec_sql, false);

	m_pLog->Output("[Acquire] 执行 DB2 的数据采集 ...");
	hdfsFS hd_fs = pHdfsConnector->GetHdfsFS();
	const int VEC_SIZE = vec_sql.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		// 从 DB2 数据库中采集结果数据
		std::vector<std::vector<std::string> > vec2_data;
		m_pAcqDB2->FetchEtlData(vec_sql[i], vec2_data);

		// 将采集结果数据写入 HDFS 再 load 到 HIVE
		std::string hdfsFile = GeneralHdfsFileName();
		hdfsFile = DB2DataOutputHdfsFile(vec2_data, hd_fs, hdfsFile);
		LoadHdfsFile2Hive(m_taskInfo.EtlRuleTarget, hdfsFile);
	}
}

void Acquire::CheckSourceTable(bool hive)
{
	m_pLog->Output("[Acquire] Check source table whether exist or not ...");

	bool all_src_tab_not_exist = true;			// 假定：所有源表都不存在
	std::string trans_src_tab;
	const int SRC_TAB_SIZE = m_taskInfo.vecEtlRuleDataSrc.size();

	if ( hive )		// HIVE
	{
		for ( int i = 0; i < SRC_TAB_SIZE; ++i )
		{
			trans_src_tab = TransSourceDate(m_taskInfo.vecEtlRuleDataSrc[i].srcTabName);

			// 检查源表是否存在？
			if ( !m_pAcqHive->CheckTableExisted(trans_src_tab) )	// 表不存在
			{
				m_pLog->Output("<WARNING> [Acquire] [HIVE] Source table do not exist: %s [IGNORED]", trans_src_tab.c_str());

				// 表不存在：置为无效
				m_taskInfo.vecEtlRuleDataSrc[i].isValid = false;
				m_taskInfo.vecEtlRuleDim[i].isValid = false;
				m_taskInfo.vecEtlRuleVal[i].isValid = false;
			}
			else	// 表存在
			{
				// 有存在的表
				if ( all_src_tab_not_exist )
				{
					all_src_tab_not_exist = false;
				}

				m_taskInfo.vecEtlRuleDataSrc[i].isValid = true;
				m_taskInfo.vecEtlRuleDim[i].isValid = true;
				m_taskInfo.vecEtlRuleVal[i].isValid = true;
			}
		}
	}
	else	// DB2
	{
		for ( int i = 0; i < SRC_TAB_SIZE; ++i )
		{
			trans_src_tab = TransSourceDate(m_taskInfo.vecEtlRuleDataSrc[i].srcTabName);

			// 检查源表是否存在？
			if ( !m_pAcqDB2->CheckTableExisted(trans_src_tab) )		// 表不存在
			{
				m_pLog->Output("<WARNING> [Acquire] [DB2] Source table do not exist: %s [IGNORED]", trans_src_tab.c_str());

				// 表不存在：置为无效
				m_taskInfo.vecEtlRuleDataSrc[i].isValid = false;
				m_taskInfo.vecEtlRuleDim[i].isValid = false;
				m_taskInfo.vecEtlRuleVal[i].isValid = false;
			}
			else	// 表存在
			{
				// 有存在的表
				if ( all_src_tab_not_exist )
				{
					all_src_tab_not_exist = false;
				}

				m_taskInfo.vecEtlRuleDataSrc[i].isValid = true;
				m_taskInfo.vecEtlRuleDim[i].isValid = true;
				m_taskInfo.vecEtlRuleVal[i].isValid = true;
			}
		}
	}

	// 是否所有源表都不存在？
	if ( all_src_tab_not_exist )
	{
		throw base::Exception(ACQERR_CHECK_SRC_TAB_FAILED, "[%s] All source tables do not exist ! (KPI_ID:%s, ETL_ID:%s) [FILE:%s, LINE:%d]", (hive ? "HIVE":"DB2"), m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] Check source table OK.");
}

void Acquire::LoadHdfsConfig()
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

	// 设置 HDFS 主机组合信息
	if ( m_isTest )		// 测试环境
	{
		base::PubStr::SetFormatString(m_sHdfsHostInfo, "%s:%d", m_sHdfsHost.c_str(), m_nHdfsPort);
	}
	else	// 非测试环境
	{
		m_sHdfsHostInfo = m_sHdfsHost;
	}

	// 加上末尾的斜杠
	base::BaseDir::DirWithSlash(m_sHdfsTmpPath);
}

std::string Acquire::GeneralHdfsFileName()
{
	std::string hdfs_file_name;
	base::PubStr::SetFormatString(hdfs_file_name, "%s_%s_%s%02d", m_sKpiID.c_str(), m_sEtlID.c_str(), base::SimpleTime::Now().Time14().c_str(), ++m_seqHdfsFile);
	return hdfs_file_name;
}

std::string Acquire::DB2DataOutputHdfsFile(std::vector<std::vector<std::string> >& vec2_data, hdfsFS& hd_fs, const std::string& hdfs_file)
{
	const std::string FULL_FILE_PATH = m_sHdfsTmpPath + hdfs_file;

	hdfsFile hd_file = hdfsOpenFile(hd_fs, FULL_FILE_PATH.c_str(), O_WRONLY|O_CREAT, 0, 0, 0);
	if ( NULL == hd_file )
	{
		throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Open file failed: %s, ERROR: %s [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), strerror(errno), __FILE__, __LINE__);
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
			throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Write file failed: %s (index:%llu), ERROR: %s [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), i, strerror(errno), __FILE__, __LINE__);
		}

		// 每一千行，flush一次buffer
		if ( i % 1000 == 0 && i != 0 )
		{
			if ( hdfsFlush(hd_fs, hd_file) < 0 )
			{
				throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Flush file failed: %s (index:%llu), ERROR: %s [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), i, strerror(errno), __FILE__, __LINE__);
			}
		}
	}

	// 最后再flush一次
	if ( hdfsFlush(hd_fs, hd_file) < 0 )
	{
		throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Finally flush file failed: %s, ERROR: %s [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), strerror(errno), __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] [HDFS] Write file line(s): %llu", VEC2_SIZE);

	if ( hdfsCloseFile(hd_fs, hd_file) < 0 )
	{
		throw base::Exception(ACQERR_OUTPUT_HDFS_FILE_FAILED, "[Acquire] [HDFS] Close file failed: %s, ERROR: %s [FILE:%s, LINE:%d]", FULL_FILE_PATH.c_str(), strerror(errno), __FILE__, __LINE__);
	}
	m_pLog->Output("[Acquire] [HDFS] Close file [%s].", FULL_FILE_PATH.c_str());

	return FULL_FILE_PATH;
}

void Acquire::LoadHdfsFile2Hive(const std::string& target_tab, const std::string& hdfs_file)
{
	if ( hdfs_file.empty() )
	{
		throw base::Exception(ACQERR_LOAD_HIVE_FAILED, "[Acquire] HDFS文件名无效！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire] Load HDFS file [%s] to HIVE ...", hdfs_file.c_str());

	std::string load_sql;
	base::PubStr::SetFormatString(load_sql, "load data inpath '%s' into table %s", 
											hdfs_file.c_str(), 
											target_tab.c_str());

	std::vector<std::string> vec_sql;
	vec_sql.push_back(load_sql);
	m_pAcqHive->ExecuteAcqSQL(vec_sql);

	m_pLog->Output("[Acquire] Load HDFS file to HIVE ---- done!");
}

int Acquire::RebuildHiveTable()
{
	std::vector<std::string> vec_field;
	TaskInfo2TargetFields(vec_field);

	if ( m_bRetainResult )		// 保留旧的结果数据
	{
		// 检查 HIVE 目标表是否存在
		if ( m_pAcqHive->CheckTableExisted(m_taskInfo.EtlRuleTarget) )
		{
			m_pLog->Output("[Acquire] HIVE 采集目标表 (%s) 已存在：Do nothing!", m_taskInfo.EtlRuleTarget.c_str());
		}
		else
		{
			m_pLog->Output("[Acquire] 创建 HIVE 采集目标表 (%s)", m_taskInfo.EtlRuleTarget.c_str());
			m_pAcqHive->RebuildTable(m_taskInfo.EtlRuleTarget, vec_field, m_hiveTabLocation);
			m_pLog->Output("[Acquire] HIVE 采集目标表 [%s] 创建完成!", m_taskInfo.EtlRuleTarget.c_str());
		}
	}
	else	// 清空结果数据
	{
		m_pLog->Output("[Acquire] 重建 HIVE 采集目标表 (%s)", m_taskInfo.EtlRuleTarget.c_str());
		m_pAcqHive->RebuildTable(m_taskInfo.EtlRuleTarget, vec_field, m_hiveTabLocation);
		m_pLog->Output("[Acquire] HIVE 采集目标表 [%s] 重建完成!", m_taskInfo.EtlRuleTarget.c_str());
	}

	return vec_field.size();
}

void Acquire::TaskInfo2TargetFields(std::vector<std::string>& vec_field)
{
	if ( m_taskInfo.vecEtlRuleDim.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "没有采集维度信息! 无法生成目标表字段! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	if ( m_taskInfo.vecEtlRuleVal.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "没有采集值信息! 无法生成目标表字段! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	std::vector<std::string> v_field;

	// 只取第一组采集维度，作为目标表字段的依据
	AcqEtlDim& first_dim = m_taskInfo.vecEtlRuleDim[0];
	size_t vec_size = first_dim.vecEtlDim.size();
	for ( size_t i = 0; i < vec_size; ++i )
	{
		OneEtlDim& one = first_dim.vecEtlDim[i];

		// 忽略无效维度
		if ( one.EtlDimSeq < 0 )
		{
			m_pLog->Output("[目标表字段] 忽略维度: DIM_ID=[%s], DIM_SEQ=[%d], DIM_NAME=[%s], DIM_SRCNAME=[%s], DIM_MEMO=[%s]",
				one.EtlDimID.c_str(), one.EtlDimSeq, one.EtlDimName.c_str(), one.EtlDimSrcName.c_str(), one.GetDimMemoTypeStr().c_str());
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
	AcqEtlVal& first_val = m_taskInfo.vecEtlRuleVal[0];
	vec_size = first_val.vecEtlVal.size();
	for ( size_t i = 0; i < vec_size; ++i )
	{
		OneEtlVal& one = first_val.vecEtlVal[i];

		// 忽略无效值
		if ( one.EtlValSeq < 0 )
		{
			m_pLog->Output("[目标表字段] 忽略值: VAL_ID=[%s], VAL_SEQ=[%d], VAL_NAME=[%s], VAL_SRCNAME=[%s], VAL_MEMO=[%s]",
				one.EtlValID.c_str(), one.EtlValSeq, one.EtlValName.c_str(), one.EtlValSrcName.c_str(), one.GetValMemoTypeStr().c_str());
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

void Acquire::TaskInfo2Sql(std::vector<std::string>& vec_sql, bool hive)
{
	switch ( m_taskInfo.EtlCondType )
	{
		case AcqTaskInfo::ETLCTYPE_NONE:					// 不带条件
		case AcqTaskInfo::ETLCTYPE_STRAIGHT:				// 直接条件
			m_pLog->Output("[Acquire] 采集规则的条件类型：%s", (AcqTaskInfo::ETLCTYPE_NONE == m_taskInfo.EtlCondType ? "不带条件":"直接条件"));
			NoneOrStraight2Sql(vec_sql, hive);
			break;
		case AcqTaskInfo::ETLCTYPE_OUTER_JOIN:				// 外连条件
			m_pLog->Output("[Acquire] 采集规则的条件类型：%s", "外连条件");
			OuterJoin2Sql(vec_sql, hive);
			break;
		case AcqTaskInfo::ETLCTYPE_OUTER_JOIN_WITH_COND:	// 外连加条件
			m_pLog->Output("[Acquire] 采集规则的条件类型：%s", "外连加条件");
			OuterJoin2Sql(vec_sql, hive, true);
			break;
		case AcqTaskInfo::ETLCTYPE_UNKNOWN:		// 未知类型
		default:
			throw base::Exception(ACQERR_TASKINFO_INVALID, "采集规则解析失败：未知的采集条件类型! (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
}

void Acquire::OuterJoin2Sql(std::vector<std::string>& vec_sql, bool hive, bool with_cond /*= false*/)
{
	// 分析采集条件
	// (不带条件)格式：[外连表名]:[关联的维度字段(逗号分隔)]
	// (带条件)格式：[外连表名]:[关联的维度字段(逗号分隔)]:[Hive SQL条件语句(带或者不带where都可以)]
	std::string& etl_cond = m_taskInfo.EtlCondition;
	base::PubStr::Trim(etl_cond);

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(etl_cond, ":", vec_str);
	if ( (with_cond && vec_str.size() != 3) 		// 带条件，但格式不正确
		|| (!with_cond && vec_str.size() != 2) )	// 不带条件，但格式不正确
	{
		throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：无法识别的采集条件 [%s] (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", etl_cond.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	std::string outer_table = vec_str[0];		// 外连表名
	if ( outer_table.empty() )
	{
		throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：无法识别的采集条件 [%s] (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", etl_cond.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	// 外连表名也可通过采集周期来指定
	outer_table = TransSourceDate(outer_table);

	// 检查外连表是否存在？
	m_pLog->Output("[Acquire] Check outer table whether exist or not ...");
	std::vector<std::string> vec_outer_tabname;
	TaskInfoUtil::GetTableNames(outer_table, vec_outer_tabname);
	const int OUTER_TABNAME_SIZE = vec_outer_tabname.size();
	for ( int i = 0; i < OUTER_TABNAME_SIZE; ++i )
	{
		std::string& ref_outer_tabname = vec_outer_tabname[i];
		if ( (hive && !m_pAcqHive->CheckTableExisted(ref_outer_tabname)) 			// HIVE
				|| (!hive && !m_pAcqDB2->CheckTableExisted(ref_outer_tabname)) )	// DB2
		{
			const std::string STR_IDENT = (hive ? "[HIVE]" : "[DB2]");
			m_pLog->Output("<WARNING> [Acquire] %s Outer table did not exist: %s !", STR_IDENT.c_str(), ref_outer_tabname.c_str());

			throw base::Exception(ACQERR_CHECK_OUTER_TAB_FAILED, "%s Outer table '%s' did not exist ! (KPI_ID:%s, ETL_ID:%s) [FILE:%s, LINE:%d]", STR_IDENT.c_str(), ref_outer_tabname.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
		}
	}

	// 带条件
	std::string sql_cond;
	if ( with_cond )
	{
		sql_cond = GetSQLCondition(vec_str[2]);
	}

	// 拆分关联字段
	const std::string OUTER_ON = vec_str[1];
	base::PubStr::Str2StrVector(OUTER_ON, ",", vec_str);
	if ( vec_str.empty() )
	{
		throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：无法识别的采集条件 [%s] (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", etl_cond.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	std::string sql;
	std::vector<std::string> v_sql;

	const int OUTER_ON_SIZE = vec_str.size();
	const int SRC_SIZE = m_taskInfo.vecEtlRuleDataSrc.size();
	if ( 1 == SRC_SIZE )		// 单个源数据
	{
		const int NUM_JOIN_ON = TaskInfoUtil::GetNumOfEtlDimJoinOn(m_taskInfo.vecEtlRuleDim[0]);
		if ( OUTER_ON_SIZE != NUM_JOIN_ON )
		{
			throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：采集条件的关联字段数 [%d] 与 维度规则的关联字段数 [%d] (ETLRULE_ID:%s, ETLDIM_ID:%s) [FILE:%s, LINE:%d]", OUTER_ON_SIZE, NUM_JOIN_ON, m_taskInfo.EtlRuleID.c_str(), m_taskInfo.vecEtlRuleDim[0].acqEtlDimID.c_str(), __FILE__, __LINE__);
		}

		const std::string SRC_TABLE = TransSourceDate(m_taskInfo.vecEtlRuleDataSrc[0].srcTabName);

		if ( hive )
		{
			sql += "insert " + m_sInsertMode + " table " + m_taskInfo.EtlRuleTarget + " ";
		}

		sql += TaskInfoUtil::GetOuterJoinEtlSQL(m_taskInfo.vecEtlRuleDim[0], m_taskInfo.vecEtlRuleVal[0], SRC_TABLE, outer_table, vec_str, "");
	}
	else	// 多个源数据
	{
		std::string target_dim_sql = TaskInfoUtil::GetTargetDimSql(m_taskInfo.vecEtlRuleDim[0]);

		// Hive SQL head
		if ( hive )
		{
			sql += "insert " + m_sInsertMode + " table " + m_taskInfo.EtlRuleTarget + " ";
		}

		sql += "select ";
		sql += target_dim_sql + TaskInfoUtil::GetTargetValSql(m_taskInfo.vecEtlRuleVal[0]);
		sql += " from (";

		int num_join_on = 0;
		std::string src_table;

		// Hive SQL body
		bool is_first = true;
		std::string sub_cond;
		std::map<int, EtlSrcInfo>::iterator m_it;
		for ( int i = 0; i < SRC_SIZE; ++i )
		{
			DataSource& ref_datasrc = m_taskInfo.vecEtlRuleDataSrc[i];
			AcqEtlDim& ref_dim = m_taskInfo.vecEtlRuleDim[i];
			AcqEtlVal& ref_val = m_taskInfo.vecEtlRuleVal[i];

			// 是否有效
			if ( !ref_datasrc.isValid || !ref_dim.isValid || !ref_val.isValid )
			{
				continue;
			}

			if ( is_first )
			{
				is_first = false;
			}
			else
			{
				sql += " union all ";
			}

			// 获取子条件
			if ( (m_it = m_taskInfo.mapEtlSrc.find(i+1)) != m_taskInfo.mapEtlSrc.end() )
			{
				sub_cond = GetSQLCondition(m_it->second.condition);
			}
			else
			{
				sub_cond.clear();
			}

			num_join_on = TaskInfoUtil::GetNumOfEtlDimJoinOn(ref_dim);
			if ( OUTER_ON_SIZE != num_join_on )
			{
				throw base::Exception(ACQERR_OUTER_JOIN_FAILED, "采集规则解析失败：采集条件的关联字段数 [%d] 与 维度规则的关联字段数 [%d] (ETLRULE_ID:%s, ETLDIM_ID:%s) [FILE:%s, LINE:%d]", OUTER_ON_SIZE, num_join_on, m_taskInfo.EtlRuleID.c_str(), ref_dim.acqEtlDimID.c_str(), __FILE__, __LINE__);
			}

			src_table = TransSourceDate(ref_datasrc.srcTabName);

			sql += TaskInfoUtil::GetOuterJoinEtlSQL(ref_dim, ref_val, src_table, outer_table, vec_str, sub_cond);
		}

		// Hive SQL tail
		sql += ") TMP group by " + target_dim_sql;
	}

	// 加上条件
	if ( with_cond )
	{
		const size_t GROUP_POS = base::PubStr::UpperB(sql).rfind(" GROUP ");
		sql.insert(GROUP_POS, sql_cond);
	}

	v_sql.push_back(sql);
	v_sql.swap(vec_sql);
}

void Acquire::NoneOrStraight2Sql(std::vector<std::string>& vec_sql, bool hive)
{
	std::string condition;
	// 是否为直接条件
	if ( AcqTaskInfo::ETLCTYPE_STRAIGHT == m_taskInfo.EtlCondType )
	{
		condition = GetSQLCondition(m_taskInfo.EtlCondition);
	}

	std::string sql;
	std::vector<std::string> v_sql;

	const int SRC_SIZE = m_taskInfo.vecEtlRuleDataSrc.size();
	if ( 1 == SRC_SIZE )	// 单个源数据
	{
		if ( hive )
		{
			sql += "insert " + m_sInsertMode + " table " + m_taskInfo.EtlRuleTarget + " ";
		}

		sql += "select ";
		sql += TaskInfoUtil::GetEtlDimSql(m_taskInfo.vecEtlRuleDim[0], true) + TaskInfoUtil::GetEtlValSql(m_taskInfo.vecEtlRuleVal[0]);
		sql += " from " + TransSourceDate(m_taskInfo.vecEtlRuleDataSrc[0].srcTabName) + condition;
		sql += " group by " + TaskInfoUtil::GetEtlDimSql(m_taskInfo.vecEtlRuleDim[0], false);
	}
	else	// 多个源数据
	{
		const std::string TARGET_DIM_SQL = TaskInfoUtil::GetTargetDimSql(m_taskInfo.vecEtlRuleDim[0]);

		// SQL head
		if ( hive )
		{
			sql += "insert " + m_sInsertMode + " table " + m_taskInfo.EtlRuleTarget + " ";
		}

		sql += "select ";
		sql += TARGET_DIM_SQL + TaskInfoUtil::GetTargetValSql(m_taskInfo.vecEtlRuleVal[0]);
		sql += " from (";

		// SQL body
		//std::string tab_alias;
		//std::string tab_pre;
		bool is_first = true;
		std::string sub_cond;
		std::map<int, EtlSrcInfo>::iterator m_it;
		for ( int i = 0; i < SRC_SIZE; ++i )
		{
			DataSource& ref_datasrc = m_taskInfo.vecEtlRuleDataSrc[i];
			AcqEtlDim& ref_dim      = m_taskInfo.vecEtlRuleDim[i];
			AcqEtlVal& ref_val      = m_taskInfo.vecEtlRuleVal[i];

			// 是否有效
			if ( !ref_datasrc.isValid || !ref_dim.isValid || !ref_val.isValid )
			{
				continue;
			}

			if ( is_first )
			{
				is_first = false;
				sql += "select ";
			}
			else
			{
				sql += " union all select ";
			}

			// 获取子条件
			if ( (m_it = m_taskInfo.mapEtlSrc.find(i+1)) != m_taskInfo.mapEtlSrc.end() )
			{
				sub_cond = GetSQLCondition(m_it->second.condition);
			}
			else
			{
				sub_cond.clear();
			}

			//tab_alias = base::PubStr::TabIndex2TabAlias(i);
			//tab_pre = tab_alias + ".";

			//sql += TaskInfoUtil::GetEtlDimSql(ref_dim, true, tab_pre) + TaskInfoUtil::GetEtlValSql(ref_val, tab_pre);
			//sql += " from " + TransSourceDate(ref_datasrc.srcTabName) + " " + tab_alias;
			//sql += " group by " + TaskInfoUtil::GetEtlDimSql(ref_dim, false, tab_pre);
			sql += TaskInfoUtil::GetEtlDimSql(ref_dim, true) + TaskInfoUtil::GetEtlValSql(ref_val);
			sql += " from " + TransSourceDate(ref_datasrc.srcTabName);
			sql += sub_cond + " group by " + TaskInfoUtil::GetEtlDimSql(ref_dim, false);
		}

		// SQL tail
		sql += ") TMP " + condition + " group by " + TARGET_DIM_SQL;
	}

	v_sql.push_back(sql);
	v_sql.swap(vec_sql);
}

std::string Acquire::GetSQLCondition(const std::string& cond)
{
	std::string sql_condition = base::PubStr::TrimB(cond);
	if ( !sql_condition.empty() )
	{
		// 标记替换
		ExchangeSQLMark(sql_condition);

		std::string head_where = sql_condition.substr(0, 5);
		base::PubStr::Upper(head_where);

		// 加上"where"
		if ( head_where != "WHERE" )
		{
			sql_condition.insert(0, " where ");
		}
		else
		{
			sql_condition.insert(0, " ");
		}
	}

	return sql_condition;
}

void Acquire::ExchangeSQLMark(std::string& sql)
{
	if ( NULL == m_pSQLTrans )
	{
		throw base::Exception(ACQERR_EXCHANGE_SQLMARK_FAILED, "Exchange sql mark failed: NO SQL Translator! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	std::string str_error;
	if ( !m_pSQLTrans->Translate(sql, &str_error) )
	{
		throw base::Exception(ACQERR_EXCHANGE_SQLMARK_FAILED, "Exchange sql mark failed: %s [FILE:%s, LINE:%d]", str_error.c_str(), __FILE__, __LINE__);
	}
}

std::string Acquire::TransSourceDate(const std::string& src_tabname)
{
	std::string date_mark;
	if ( base::PubTime::DT_DAY == m_acqDateType )		// 日类型
	{
		date_mark = "YYYYMMDD";
	}
	else if ( base::PubTime::DT_MONTH == m_acqDateType )	// 月类型
	{
		date_mark = "YYYYMM";
	}
	else
	{
		throw base::Exception(ACQERR_TRANS_DATASRC_FAILED, "未知的采集时间类型：%d [FILE:%s, LINE:%d]", m_acqDateType, __FILE__, __LINE__);
	}

	// 找到时间标记才进行替换
	const std::string C_TAB = base::PubStr::UpperB(src_tabname);
	std::string tab_name = src_tabname;
	size_t pos = 0;
	while ( (pos = C_TAB.find(date_mark, pos)) != std::string::npos )	// 循环替换
	{
		tab_name.replace(pos, date_mark.size(), m_acqDate);
		pos += date_mark.size();
	}

	return tab_name;
}

