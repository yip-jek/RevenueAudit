#pragma once

#include <string>
#include "exception.h"
#include "config.h"

namespace base
{

class Log;

class BaseFrameApp
{
public:
	BaseFrameApp();
	virtual ~BaseFrameApp();

public:
	virtual const char* Version();
	virtual void SetArgv(char** pp_arg);
	virtual void LoadConfig() throw(Exception) = 0;
	virtual std::string GetConfigFile();
	virtual std::string GetLogPathConfig();
	virtual void Init() throw(Exception) = 0;
	virtual void Run() throw(Exception) = 0;

protected:
	Config	m_cfg;
	Log*	m_pLog;
	char**	m_ppArgv;
};

}	// namespace base

extern base::BaseFrameApp* g_pApp;

