#include "acquire.h"
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "log.h"
#include "cacqdb2.h"
#include "chivethrift.h"


Acquire g_Acquire;


Acquire::Acquire()
:m_nKpiID(0)
,m_nEtlID(0)
,m_nHivePort(0)
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
	return ("Acquire: Version 1.00.0017 released. Compiled at "__TIME__" on "__DATE__);
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

	m_tabKpiRule = m_cfg.GetCfgValue("TABLE", "TAB_KPI_RULE");
	m_tabEtlRule = m_cfg.GetCfgValue("TABLE", "TAB_ETL_RULE");
	m_tabEtlDim  = m_cfg.GetCfgValue("TABLE", "TAB_ETL_DIM");
	m_tabEtlVal  = m_cfg.GetCfgValue("TABLE", "TAB_ETL_VAL");

	m_pLog->Output("Load configuration OK.");
}

void Acquire::Init() throw(base::Exception)
{
	GetParameterTaskInfo();

	m_pDB2 = new CAcqDB2(m_sDBName, m_sUsrName, m_sPasswd);
	if ( NULL == m_pDB2 )
	{
		throw base::Exception(ACQERR_INIT_FAILED, "new CAcqDB2 failed: 无法申请到内存空间!");
	}
	m_pAcqDB2 = dynamic_cast<CAcqDB2*>(m_pDB2);

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
	m_pAcqDB2->Connect();

	m_pCHive->Connect();

	m_pCHive->Test("audit_bdzt_20160329");

	m_pCHive->Disconnect();

	m_pAcqDB2->Disconnect();
}

void Acquire::GetParameterTaskInfo() throw(base::Exception)
{
	// 格式：启动批号|指标ID|采集规则ID|...
	std::vector<std::string> vec_str;
	boost::split(vec_str, m_ppArgv[4], boost::is_any_of("|"));

	if ( vec_str.size() < 3 )
	{
		throw base::Exception(ACQERR_TASKINFO_ERROR, "参数任务信息异常(split size:%lu), 无法按格式拆分! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	try
	{
		m_nKpiID = boost::lexical_cast<long>(vec_str[1]);
	}
	catch ( boost::bad_lexical_cast& e )
	{
		throw base::Exception(ACQERR_KPIID_INVALID, "指标ID [%s] 无效! %s [FILE:%s, LINE:%d]", vec_str[1].c_str(), e.what(), __FILE__, __LINE__);
	}

	try
	{
		m_nEtlID = boost::lexical_cast<long>(vec_str[2]);
	}
	catch ( boost::bad_lexical_cast& e )
	{
		throw base::Exception(ACQERR_ETLID_INVALID, "采集规则ID [%s] 无效! %s [FILE:%s, LINE:%d]", vec_str[2].c_str(), e.what(), __FILE__, __LINE__);
	}
}

