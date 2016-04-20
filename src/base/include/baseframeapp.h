#pragma once

#include "exception.h"

namespace base
{

class Log;

class BaseFrameApp
{
public:
	BaseFrameApp();
	virtual ~BaseFrameApp();

public:
	virtual void Init() throw(Exception);
	virtual void Run() throw(Exception);

private:
	Log*	m_pLog;
};

extern BaseFrameApp* g_pApp;

}	// namespace base

