#include "baseframeapp.h"
#include <iostream>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <boost/lexical_cast.hpp>
#include "def.h"
#include "log.h"
#include "basedb2.h"
#include "basehivethrift.h"

namespace base
{

BaseFrameApp::BaseFrameApp()
:m_pLog(Log::Instance())
,m_ppArgv(NULL)
,m_pDB2(NULL)
,m_pHiveThrift(NULL)
{
}

BaseFrameApp::~BaseFrameApp()
{
	if ( m_pDB2 != NULL )
	{
		delete m_pDB2;
		m_pDB2 = NULL;
	}

	if ( m_pHiveThrift != NULL )
	{
		delete m_pHiveThrift;
		m_pHiveThrift = NULL;
	}
}

const char* BaseFrameApp::Version()
{
	return ("BaseFrameApp: Version 1.00 released. Compiled at "__TIME__" on "__DATE__);
}

void BaseFrameApp::SetArgv(char** pp_arg)
{
	assert(pp_arg);

	m_ppArgv = pp_arg;
}

std::string BaseFrameApp::GetConfigFile()
{
	return m_ppArgv[3];
}

std::string BaseFrameApp::GetLogPathConfig()
{
	m_cfg.SetCfgFile(GetConfigFile());
	m_cfg.RegisterItem("SYS", "LOG_PATH");
	m_cfg.ReadConfig();

	return m_cfg.GetCfgValue("SYS", "LOG_PATH");
}

}	// namespace base

///////////////////////////////////////////////////////////////////////////////////////////////////
using namespace base;

BaseFrameApp* g_pApp = NULL;

int main(int argc, char* argv[])
{
	if ( argc < 5 )
	{
		std::cerr << "[usage] " << argv[0] << " daemon_flag ccm_id cfg_file [task_info]" << std::endl;
		return -1;
	}

	// Daemon process ?
	if ( strcmp(argv[1], "1") == 0 )
	{
		pid_t fpid = fork();
		if ( fpid < 0 )			// fork error!
		{
			std::cerr << "fork error: " << fpid << std::endl;
			return -1;
		}
		else if ( fpid > 0 )	// Parent process end
		{
			return 0;
		}
	}

	assert(g_pApp);

	g_pApp->SetArgv(argv);

	try
	{
		if ( !Log::SetCCMID(boost::lexical_cast<long long>(argv[2])) )
		{
			std::cerr << "[LOG] Set CCM_ID failed!" << std::endl;
			return -1;
		}
	}
	catch ( boost::bad_lexical_cast& e )
	{
		std::cerr << e.what() << std::endl;
		return -1;
	}

	AutoLogger aLog;
	Log* pLog = aLog.Get();

	try
	{
		pLog->SetPath(g_pApp->GetLogPathConfig());
		pLog->Init();

		std::cout << g_pApp->Version() << std::endl;
		pLog->Output(g_pApp->Version());

		g_pApp->LoadConfig();
		g_pApp->Init();
		g_pApp->Run();
	}
	catch ( Exception& ex )
	{
		std::cerr << "[ERROR] " << ex.What() << ", ERROR_CODE: " << ex.ErrorCode() << std::endl;
		pLog->Output("[ERROR] %s, ERROR_CODE: %d", ex.What().c_str(), ex.ErrorCode());
		return -1;
	}
	catch ( ... )
	{
		std::cerr << "[ERROR] Unknown error! [FILE:" << __FILE__ << ", LINE:" << __LINE__ << "]" << std::endl;
		pLog->Output("[ERROR] Unknown error! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		return -1;
	}

	std::cout << argv[0] << " quit!" << std::endl;
	pLog->Output("%s quit!", argv[0]);
	return 0;
}
