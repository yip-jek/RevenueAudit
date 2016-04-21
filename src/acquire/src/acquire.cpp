#include "acquire.h"
#include <vector>
#include "log.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>


Acquire g_Acquire;


Acquire::Acquire()
:m_nKpiID(0)
,m_nEtlID(0)
,m_nHivePort(0)
,m_pTHiveClient(NULL)
{
	g_pApp = &g_Acquire;
}

Acquire::~Acquire()
{
	Release();
}

const char* Acquire::Version()
{
	return ("Acquire: Version 1.00.0014 released. Compiled at "__TIME__" on "__DATE__);
}

void Acquire::LoadConfig() throw(base::Exception)
{
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
	m_nHivePort = (int)m_cfg.GetCfgLongVal("HIVE_SERVER", "PORT");
	if ( m_nHivePort <= 0 )
	{
		throw base::Exception(ACQERR_HIVE_PORT_INVALID, "Hive服务器端口无效! (port=%d) [FILE:%s, LINE:%d]", m_nHivePort, __FILE__, __LINE__);
	}

	m_pLog->Output("Load configuration OK.");
}

void Acquire::Init() throw(base::Exception)
{
	GetParameterTaskInfo();

	Release();

	m_spSocket.reset(new T_THRIFT_SOCKET(m_sHiveIP, m_nHivePort));

	m_spTransport.reset(new T_THRIFT_BUFFER_TRANSPORT(m_spSocket));

	m_spProtocol.reset(new T_THRIFT_BINARY_PROTOCOL(m_spTransport));

	m_pTHiveClient = new T_THRIFT_HIVE_CLIENT(m_spProtocol);

	m_pLog->Output("Init OK.");
}

void Acquire::Run() throw(base::Exception)
{
	try
	{
		m_spTransport->open();
		m_pLog->Output("[HIVE] Connect <Host:%s, Port:%d> OK.", m_sHiveIP.c_str(), m_nHivePort);

		std::string test_sql = "select * from test1";
		m_pLog->Output("[HIVE] Query sql: %s", test_sql.c_str());

		m_pLog->Output("[HIVE] Execute query sql ...");
		m_pTHiveClient->execute(test_sql);
		m_pLog->Output("[HIVE] Execute query sql OK.");

		std::vector<std::string> vec_str;
		long total = 0;
		do
		{
			vec_str.clear();

			m_pTHiveClient->fetchN(vec_str, HIVE_MAX_FETCHN);

			const int V_SIZE = vec_str.size();
			for ( int i = 0; i < V_SIZE; ++i )
			{
				m_pLog->Output("[GET] %d> %s", ++total, vec_str[i].c_str());
			}
		} while ( vec_str.size() > 0 );
		m_pLog->Output("[HIVE] Get result OK.");

		m_spTransport->close();
		m_pLog->Output("[HIVE] Disconnect.");
	}
	catch ( const apache::thrift::TApplicationException& ex )
	{
		throw base::Exception(ACQERR_APP_EXCEPTION, "[TApplicationException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
	catch ( const apache::thrift::TException& ex )
	{
		throw base::Exception(ACQERR_T_EXCEPTION, "[TException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

void Acquire::Release()
{
	if ( m_pTHiveClient != NULL )
	{
		delete m_pTHiveClient;
		m_pTHiveClient = NULL;
	}
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

