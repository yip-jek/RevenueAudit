#include "baseframeapp.h"
#include <iostream>
#include <string.h>
#include <unistd.h>
#include <boost/cast.hpp>
#include "def.h"
#include "log.h"
#include "config.h"

namespace base
{

BaseFrameApp::BaseFrameApp()
:m_pLog(Log::Instance())
{
}

BaseFrameApp::~BaseFrameApp()
{
}

void BaseFrameApp::Init() throw(Exception)
{
	throw Exception(BFA_INIT_FAILED, "BaseFrameApp initialization failed!");
}

void BaseFrameApp::Run() throw(Exception)
{
	throw Exception(BFA_EXECUTE_FAILED, "BaseFrameApp execution failed!");
}

///////////////////////////////////////////////////////////////////////////////////////////////////
BaseFrameApp* g_pApp = NULL;

int main(int argc, char* argv[])
{
	if ( argc < 3 )
	{
		std::cerr << "[usage] " << argv[0] << " daemon_flag ccm_id ..." << std::endl;
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

	ASSERT(g_pApp);

	try
	{
		if ( !Log::SetCCMID(boost::numeric_cast<long long>(argv[2])) )
		{
			std::cerr << "[LOG] Set CCM_ID failed!" << std::endl;
			return -1;
		}
	}
	catch ( boost::bad_numeric_cast& e )
	{
		std::cerr << e.what() << std::endl;
		return -1;
	}

	AutoLogger aLog;
	Log* pLog = aLog.Get();

	try
	{
		Config cfg;
		cfg.SetCfgFile([
		g_pApp->Init();
		g_pApp->Run();
	}
	catch ( Exception& ex )
	{
	}

	return 0;
}

}	// namespace base

