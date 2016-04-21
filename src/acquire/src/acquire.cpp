#include "acquire.h"
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "log.h"

Acquire g_Acquire;

Acquire::Acquire()
:m_nKpiID(0)
,m_nEtlID(0)
,m_nHivePort(0)
{
	g_pApp = &g_Acquire;
}

Acquire::~Acquire()
{
}

const char* Acquire::Version()
{
	return ("Acquire: Version 1.00.0013 released. Compiled at "__TIME__" on "__DATE__);
}

void Acquire::LoadConfig() throw(base::Exception)
{
	GetParameterTaskInfo();

	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");
	m_cfg.RegisterItem("HIVE_SERVER", "IP_ADDRESS");
	m_cfg.RegisterItem("HIVE_SERVER", "PORT");

	m_cfg.ReadConfig();

	// 数据库配置
	m_sDBName  = m_cfg.GetCfgValue("DATABASE", "DB_NAME");
	m_sUsrName = m_cfg.GetCfgValue("DATABASE", "USER_NAME");
	m_sPasswd  = m_cfg.GetCfgValue("DATABASE", "PASSWORD");

	// Hive服务器配置
	m_sHiveIP   = m_cfg.GetCfgValue("HIVE_SERVER", "IP_ADDRESS");
	m_nHivePort = m_cfg.GetCfgLongVal("HIVE_SERVER", "PORT");
	if ( m_nHivePort <= 0 )
	{
		throw Exception(ACQERR_HIVE_PORT_INVALID, "Hive服务器端口无效! (port=%d) [FILE:%s, LINE:%d]", m_nHivePort, __FILE__, __LINE__);
	}
}

void Acquire::Init() throw(base::Exception)
{
}

void Acquire::Run() throw(base::Exception)
{
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

