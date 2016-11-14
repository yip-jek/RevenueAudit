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

	// 设置日志ID
	static bool SetCCMID(long long ccm_id);

	// 重置日志文件大小
	static bool ResetFileSize(unsigned long long fsize);

	// 设置日志文件名称前缀
	static void SetLogFilePrefix(const std::string& log_prefix);

public:
	// 初始化
	void Init() throw(Exception);

	// 设置日志路径
	void SetPath(const std::string& path) throw(Exception);

	// 输出日志...
	bool Output(const char* format, ...);

private:
	// 打开新的日志
	void OpenNewLogger() throw(Exception);

private:
	static int                _sInstances;					// 实例计数器
	static Log*               _spLogger;					// 实例指针
	static long long          _sLogCcmID;					// 日志ID
	static unsigned long long _sMaxLogFileSize;				// 最大日志文件大小
	static std::string        _sLogFilePrefix;				// 日志文件名称前缀

private:
	std::string   m_sLogPath;				// 日志路径
	BaseFile      m_bfLogger;				// 日志文件类
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

