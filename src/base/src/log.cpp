#include "log.h"
#include <iostream>
#include <stdio.h>
#include "simpletime.h"
#include "pubstr.h"
#include "basedir.h"

namespace base
{

int Log::_sInstances = 0;
Log* Log::_spLogger = NULL;		// single log pointer

Log::Log()
:m_llLineCount(0)
{
}

Log::~Log()
{
	if ( m_iLogTimer.IsRunning() )
	{
		m_iLogTimer.Stop();
	}
}

Log* Log::Instance()
{
	if ( NULL == _spLogger )
	{
		std::cout << "[LOG] Creating logger instance ..." << std::endl;
		_spLogger = new Log();
	}

	// 计数加 1
	++_sInstances;
	return _spLogger;
}

void Log::Release()
{
	if ( _spLogger != NULL )
	{
		// 计数减 1
		--_sInstances;

		// 计数器归 0，则释放资源
		if ( _sInstances <= 0 )
		{
			std::cout << "[LOG] Releasing logger instance ..." << std::endl;

			delete _spLogger;
			_spLogger = NULL;
		}
	}
}

void Log::Init(const LogStrategy& strategy)
{
	AutoLock a_lock(m_mLock);

	ImportStrategy(strategy);

	StartTimer();

	OpenNewLogger();
}

bool Log::Output(const char* format, ...)
{
	if ( !m_bfLogger.IsOpen() )
	{
		return false;
	}

	AutoLock a_lock(m_mLock);

	char buf[MAX_BUFFER_SIZE] = "";
	va_list arg_ptr;
	va_start(arg_ptr, format);
	vsnprintf(buf, sizeof(buf), format, arg_ptr);
	va_end(arg_ptr);

	std::string str_out = SimpleTime::Now().LLTimeStamp() + std::string("\x20\x20") + buf;
	m_bfLogger.Write(str_out);
	++m_llLineCount;

	// Maximum file ?
	if ( CheckFileMaximum(str_out) )
	{
		m_bfLogger.Write(str_out);

		OpenNewLogger();
	}

	return true;
}

void Log::Notify()
{
	AutoLock a_lock(m_mLock);

	m_bfLogger.Write("\n\n\x20\x20\x20\x20( INTERVAL TIME END )");

	OpenNewLogger();
}

void Log::ImportStrategy(const LogStrategy& strategy)
{
	// 自动建日志路径
	if ( !BaseDir::CreateFullPath(strategy.log_path) )
	{
		throw Exception(LG_INIT_FAIL, "[LOG] The log path is invalid: [%s] [FILE:%s, LINE:%d]", strategy.log_path.c_str(), __FILE__, __LINE__);
	}

	if ( strategy.log_prefix.empty() )
	{
		throw Exception(LG_INIT_FAIL, "[LOG] The log prefix is not specified! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_strategy = strategy;
	BaseDir::DirWithSlash(m_strategy.log_path);
}

void Log::StartTimer()
{
	if ( !m_iLogTimer.SetIntervalType(m_strategy.interval) )
	{
		throw Exception(LG_INIT_FAIL, "[LOG] Unknown interval type of log timer: [%s] [FILE:%s, LINE:%d]", m_strategy.interval.c_str(), __FILE__, __LINE__);
	}

	m_iLogTimer.Register(this);
	m_iLogTimer.Start();
}

bool Log::CheckFileMaximum(std::string& tail)
{
	// Max line ?
	if ( m_strategy.max_line > 0 && m_llLineCount >= m_strategy.max_line )
	{
		tail = "\n\n\x20\x20\x20\x20( MAXIMUM FILE LINE )";
		return true;
	}

	// Max file size ?
	if ( m_strategy.max_size > 0 && m_bfLogger.FileSize() >= m_strategy.max_size )
	{
		tail = "\n\n\x20\x20\x20\x20( MAXIMUM FILE SIZE )";
		return true;
	}

	return false;
}

void Log::OpenNewLogger()
{
	if ( m_bfLogger.IsOpen() )
	{
		m_bfLogger.Close();
	}

	int log_count = 0;
	std::string fullLogPath;

	do
	{
		PubStr::SetFormatString(fullLogPath, "%s%s_%lld_%s_%04d.log", m_strategy.log_path.c_str(), m_strategy.log_prefix.c_str(), m_strategy.log_id, m_iLogTimer.GetIntervalTypeTime().c_str(), log_count++);

	} while ( BaseFile::IsFileExist(fullLogPath) );

	if ( !m_bfLogger.Open(fullLogPath) )
	{
		throw Exception(LG_OPEN_FILE_FAIL, "[LOG] Can not open log file: %s [FILE:%s, LINE:%d]", fullLogPath.c_str(), __FILE__, __LINE__);
	}

	WriteLogHeaders();
}

void Log::WriteLogHeaders()
{
	std::string str_out;
	const unsigned int V_HEADER_SIZE = m_strategy.log_headers.size();
	for ( unsigned int u = 0; u < V_HEADER_SIZE; ++u )
	{
		str_out = SimpleTime::Now().LLTimeStamp() + std::string("\x20\x20") + m_strategy.log_headers[u];
		m_bfLogger.Write(str_out);
	}

	m_llLineCount = V_HEADER_SIZE;
}

}	// namespace base

