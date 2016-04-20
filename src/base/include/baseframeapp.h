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
	virtual void LoadConfig();
	virtual std::string GetLogPathConfig();
	virtual void Init() throw(Exception);
	virtual void Run() throw(Exception);

protected:
	Config	m_cfg;
	Log*	m_pLog;
	char**	m_ppArgv;
};

}	// namespace base

extern base::BaseFrameApp* g_pApp;

