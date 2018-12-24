#pragma once

#include <string>
#include <vector>
#include <mutex>
#include "def.h"
#include "exception.h"
#include "basefile.h"
#include "intervaltimer.h"

namespace base
{

// 日志策略
struct LogStrategy
{
	LogStrategy(): log_id(0), max_size(0), max_line(0)
	{}

	std::string              log_path;				// 日志路径
	std::string              log_prefix;			// 日志名称前缀
	unsigned long long       log_id;				// 日志ID
	unsigned long long       max_size;				// 日志文件最大size
	unsigned long long       max_line;				// 日志文件最大行数
	std::string              interval;				// 日志产生时间间隔
	std::vector<std::string> log_headers;			// 日志头
};

// 日志类
class Log : public TimerInterface
{
private:
	Log();
	virtual ~Log();

public:
	// 日志缓冲区大小
	static const unsigned int MAX_BUFFER_SIZE = 4096;

public:
	// 获取多少次，就要释放多少次
	// 获取日志实例
	static Log* Instance();
	// 释放日志实例
	static void Release();

public:
	// 初始化
	void Init(const LogStrategy& strategy);

	// 输出日志...
	bool Output(const char* format, ...);

	// 接收通知消息
	virtual void Notify();

private:
	// 导入日志策略
	void ImportStrategy(const LogStrategy& strategy);

	// 开始计时器
	void StartTimer();

	// 检查最大文件条件
	bool CheckFileMaximum(std::string& tail);

	// 打开新的日志
	void OpenNewLogger();

	// 写入文件头
	void WriteLogHeaders();

private:
	static int         _sInstances;				// 实例计数器
	static Log*        _spLogger;				// 实例指针

private:
	LogStrategy        m_strategy;				// 日志策略
	std::mutex         m_mLock;					// 互斥锁
	BaseFile           m_bfLogger;				// 日志文件类
	unsigned long long m_llLineCount;			// 文件行计数
	IntervalTimer      m_iLogTimer;				// 日志计时器
};

class AutoLock
{
public:
	AutoLock(const AutoLock&) = delete;
	AutoLock& operator = (const AutoLock&) = delete;

public:
	AutoLock(std::mutex& lock): m_mtx(lock)
	{
		m_mtx.lock();
	}
	~AutoLock()
	{
		m_mtx.unlock();
	}

private:
	std::mutex& m_mtx;
};

class AutoLogger
{
public:
	AutoLogger(): m_pLogger(Log::Instance())
	{}

	virtual ~AutoLogger()
	{ Log::Release(); }

public:
	Log* Get() const
	{ return m_pLogger; }

private:
	Log*	m_pLogger;
};

}	// namespace base

