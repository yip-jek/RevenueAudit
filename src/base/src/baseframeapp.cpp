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
#include "factory.h"
//#include "TaskState.h"

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
	return ("BaseFrameApp: Version 2.00 released. Compiled at "__TIME__" on "__DATE__);
}

void BaseFrameApp::SetArgv(char** pp_arg)
{
	assert(pp_arg);

	m_ppArgv = pp_arg;
}

std::string BaseFrameApp::GetConfigFile()
{
	return m_ppArgv[5];
}

std::string BaseFrameApp::GetLogPathConfig()
{
	m_cfg.SetCfgFile(GetConfigFile());
	m_cfg.RegisterItem("SYS", "LOG_PATH");
	m_cfg.ReadConfig();

	return m_cfg.GetCfgValue("SYS", "LOG_PATH");
}

std::string BaseFrameApp::GetTaskParaInfo()
{
	return m_ppArgv[6];
}

}	// namespace base

///////////////////////////////////////////////////////////////////////////////////////////////////
using namespace base;

int main(int argc, char* argv[])
{
	if ( argc < 7 )
	{
		std::cerr << "[usage] " << argv[0] << " daemon_flag log_id mode variant cfg_file [task_info]" << std::endl;
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

	long long log_id = 0L;
	if ( !PubStr::Str2LLong(argv[2], log_id) )
	{
		std::cerr << "[ERROR] [MAIN] Invalid log ID: " << argv[2] << std::endl;
		return -1;
	}
	if ( !Log::SetLogID(log_id) )
	{
		std::cerr << "[LOG] Set log ID failed !" << std::endl;
		return -1;
	}

	assert(g_pFactory);
	FactoryAssist fa(g_pFactory);

	std::string str_error;
	BaseFrameApp* pApp = g_pFactory->CreateApp(argv[3], argv[4], &str_error);
	if ( NULL == pApp )
	{
		std::cerr << "[ERROR] [MAIN] " << str_error << std::endl;
		return -1;
	}

	pApp->SetArgv(argv);

	// 设置日志文件名称前缀
	Log::SetLogFilePrefix(pApp->GetLogFilePrefix());

	AutoLogger aLog;
	Log* pLog = aLog.Get();

	//TaskState ts(argv[4]);
	//ts.start();

	try
	{
		pLog->SetPath(pApp->GetLogPathConfig());
		pLog->Init();

		std::cout << pApp->Version() << std::endl;
		pLog->Output(pApp->Version());

		pApp->LoadConfig();
		pApp->Init();
		pApp->Run();
	}
	catch ( Exception& ex )
	{
		pApp->End(ex.ErrorCode(), ex.What());

		std::cerr << "[ERROR] " << ex.What() << ", ERROR_CODE: " << ex.ErrorCode() << std::endl;
		pLog->Output("[ERROR] %s, ERROR_CODE: %d", ex.What().c_str(), ex.ErrorCode());

		//// 上报错误码
		//std::string str_err;
		//PubStr::SetFormatString(str_err, "%s_ERROR_CODE:%d", argv[2], ex.ErrorCode());
		//ts.abort(str_err);
		return -1;
	}
	catch ( ... )
	{
		pApp->End(-1, "Unknown error!");

		std::cerr << "[ERROR] Unknown error! [FILE:" << __FILE__ << ", LINE:" << __LINE__ << "]" << std::endl;
		pLog->Output("[ERROR] Unknown error! [FILE:%s, LINE:%d]", __FILE__, __LINE__);

		//// 上报错误
		//std::string str_err;
		//PubStr::SetFormatString(str_err, "%s_ERROR:Unknown_error", argv[2]);
		//ts.abort(str_err);
		return -1;
	}

	pApp->End(0);

	std::cout << argv[0] << " quit!" << std::endl;
	pLog->Output("%s quit!", argv[0]);

	//ts.done();
	return 0;
}

