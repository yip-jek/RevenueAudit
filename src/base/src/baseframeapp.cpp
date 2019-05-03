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
:m_isTest(false)
,m_pLog(Log::Instance())
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
	return ("BaseFrameApp: Version 3.00 released. Compiled at " __TIME__ " on " __DATE__ ", by G++-" __VERSION__);
}

void BaseFrameApp::SetTestFlag(bool is_test)
{
	m_isTest = is_test;
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

void BaseFrameApp::ExportLogConfig(LogStrategy& ls)
{
	m_cfg.SetCfgFile(GetConfigFile());
	m_cfg.RegisterItem("LOG", "PATH");
	m_cfg.RegisterItem("LOG", "MAX_LINE");
	m_cfg.RegisterItem("LOG", "MAX_SIZE");
	m_cfg.RegisterItem("LOG", "INTERVAL");
	m_cfg.ReadConfig();

	ls.log_path = m_cfg.GetCfgValue("LOG", "PATH");
	ls.max_line = m_cfg.GetCfgLongVal("LOG", "MAX_LINE");
	ls.max_size = m_cfg.GetCfgLongVal("LOG", "MAX_SIZE");
	ls.interval = m_cfg.GetCfgValue("LOG", "INTERVAL");
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
	bool daemon_proc = (strcmp(argv[1], "1") == 0);
	if ( daemon_proc )
	{
		pid_t fpid = fork();
		if ( fpid < 0 )			// fork error!
		{
			std::cerr << "Process fork error: " << fpid << std::endl;
			return -1;
		}
		else if ( fpid > 0 )	// Parent process end
		{
			std::cout << "[DAEMON] Parent process [pid=" << getpid() << "] end" << std::endl;
			std::cout << "[DAEMON] Child process [pid=" << fpid << "] start" << std::endl;
			return 0;
		}
	}

	long long log_id = 0L;
	if ( !PubStr::Str2LLong(argv[2], log_id) )
	{
		std::cerr << "[ERROR] [MAIN] Invalid log ID: " << argv[2] << std::endl;
		return -1;
	}

	LogStrategy log_strategy;
	log_strategy.log_id = log_id;

	assert(g_pFactory);
	FactoryAssist fa(g_pFactory);

	std::string str_tmp;
	BaseFrameApp* pApp = g_pFactory->Create(argv[3], argv[4], &str_tmp);
	if ( NULL == pApp )
	{
		std::cerr << "[ERROR] [MAIN] " << str_tmp << std::endl;
		return -1;
	}

	pApp->SetArgv(argv);

	PubStr::SetFormatString(str_tmp, "%s: PID=[%d]", (daemon_proc?"守护进程":"一般进程"), getpid());
	log_strategy.log_headers.push_back(str_tmp);
	log_strategy.log_headers.push_back(pApp->Version());
	log_strategy.log_prefix = pApp->GetLogFilePrefix();

	AutoLogger aLog;
	Log* pLog = aLog.Get();

	//TaskState ts(argv[4]);
	//ts.start();

	try
	{
		try
		{
			pApp->ExportLogConfig(log_strategy);
			pLog->Init(log_strategy);
			std::cout << pApp->Version() << std::endl;

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

	//ts.done();
	return 0;
}

