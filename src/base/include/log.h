#pragma once

#include <string>
#include "def.h"
#include "exception.h"
#include "basefile.h"

namespace base
{

class Log
{
private:
	Log();
	~Log();

public:
	// 日志缓冲区大小
	static const unsigned int MAX_BUFFER_SIZE = 4096;

public:
	// 获取多少次，就要释放多少次
	// 获取日志实例
	static Log* Instance();
	// 释放日志实例
	static void Release();

	static bool SetCCMID(long long ccm_id);
	static bool ResetFileSize(unsigned long long fsize);

public:
	void Init() throw(Exception);
	void SetPath(const std::string& path) throw(Exception);
	bool Output(const char* format, ...);

private:
	void OpenNewLogger() throw(Exception);

private:
	static int                _sInstances;					// 实例计数器
	static Log*               _spLogger;					// 实例指针
	static long long          _sLogCcmID;
	static unsigned long long _sMaxLogFileSize;

private:
	std::string   m_sLogPath;
	BaseFile      m_bfLogger;
};

class AutoLogger
{
public:
	AutoLogger(): m_pLogger(Log::Instance())
	{}

	~AutoLogger()
	{ Log::Release(); }

public:
	Log* Get() const
	{ return m_pLogger; }

private:
	Log*	m_pLogger;
};

}	// namespace base

