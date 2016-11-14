#include "baseframeapp.h"
#include <iostream>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include "def.h"
#include "log.h"
#include "pubstr.h"
#include "basedb2.h"
#include "basejhive.h"
#include "TaskState.h"

namespace base
{

BaseFrameApp::BaseFrameApp()
:m_pLog(Log::Instance())
,m_ppArgv(NULL)
,m_pDB2(NULL)
,m_pHive(NULL)
{
}

BaseFrameApp::~BaseFrameApp()
{
	if ( m_pDB2 != NULL )
	{
		delete m_pDB2;
		m_pDB2 = NULL;
	}

	if ( m_pHive != NULL )
	{
		delete m_pHive;
		m_pHive = NULL;
	}

	Log::Release();
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

	long long ll_ccmid = 0L;
	if ( !PubStr::T1TransT2(argv[2], ll_ccmid) )
	{
		std::cerr << "[ERROR] [MAIN] Trans \"" << argv[2] << "\" to number failed !" << std::endl;
		return -1;
	}
	if ( !Log::SetCCMID(ll_ccmid) )
	{
		std::cerr << "[LOG] Set CCM_ID failed !" << std::endl;
		return -1;
	}

	// 设置日志文件名称前缀
	Log::SetLogFilePrefix(g_pApp->GetLogFilePrefix());

	AutoLogger aLog;
	Log* pLog = aLog.Get();

	TaskState ts(argv[4]);
	ts.start();

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

		// 上报错误码
		std::string str_err;
		PubStr::SetFormatString(str_err, "%s_ERROR_CODE:%d", argv[2], ex.ErrorCode());
		ts.abort(str_err);
		return -1;
	}
	catch ( ... )
	{
		std::cerr << "[ERROR] Unknown error! [FILE:" << __FILE__ << ", LINE:" << __LINE__ << "]" << std::endl;
		pLog->Output("[ERROR] Unknown error! [FILE:%s, LINE:%d]", __FILE__, __LINE__);

		// 上报错误
		std::string str_err;
		PubStr::SetFormatString(str_err, "%s_ERROR:Unknown_error", argv[2]);
		ts.abort(str_err);
		return -1;
	}

	std::cout << argv[0] << " quit!" << std::endl;
	pLog->Output("%s quit!", argv[0]);

	ts.done();
	return 0;
}

