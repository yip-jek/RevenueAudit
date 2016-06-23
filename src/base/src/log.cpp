#include "log.h"
#include <iostream>
#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "simpletime.h"
#include "pubstr.h"

namespace base
{

int Log::_sInstances = 0;
Log* Log::_spLogger = NULL;		// single log pointer

long long Log::_sLogCcmID = 0;
unsigned long Log::_sMaxLogFileSize = 10*1024*1024;	// default log file size 10M

Log::Log()
:m_sCurrentFileSize(0)
{
}

Log::~Log()
{
	TryCloseFileLogger();
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

bool Log::SetCCMID(long long ccm_id)
{
	if ( ccm_id > 0 )
	{
		_sLogCcmID = ccm_id;
		return true;
	}

	return false;
}

bool Log::ResetFileSize(unsigned long fsize)
{
	if ( fsize > 0 )
	{
		_sMaxLogFileSize = fsize;
		return true;
	}

	return false;
}

bool Log::TryMakeDir(const std::string& path)
{
	// 目录是否存在
	if ( access(path.c_str(), F_OK) != 0 )
	{
		if ( mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) != 0 )
		{
			return false;
		}
	}

	return true;
}

void Log::Init() throw(Exception)
{
	if ( m_sLogPath.empty() )
	{
		throw Exception(LG_INIT_FAIL, "[LOG] The log path is not configured! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	TryCloseFileLogger();

	OpenNewLogger();
}

void Log::SetPath(const std::string& path) throw(Exception)
{
	if ( path.empty() )
	{
		throw Exception(LG_FILE_PATH_EMPTY, "[LOG] The log path is empty! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 自动建日志路径
	std::vector<std::string> vec_path;
	PubStr::Str2StrVector(path, "/", vec_path);

	// 去除末尾空白
	if ( vec_path[vec_path.size()-1].empty() )
	{
		vec_path.erase(vec_path.end()-1);
	}

	std::string str_path;
	const int VEC_SIZE = vec_path.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		std::string& ref_path = vec_path[i];

		if ( i != 0 )
		{
			if ( ref_path.empty() )
			{
				throw Exception(LG_FILE_PATH_INVALID, "[LOG] The log path is invalid: %s [FILE:%s, LINE:%d]", path.c_str(), __FILE__, __LINE__);
			}

			str_path += "/" + ref_path;

			if ( !TryMakeDir(str_path) )
			{
				throw Exception(LG_FILE_PATH_INVALID, "[LOG] The log path is invalid: %s [FILE:%s, LINE:%d]", path.c_str(), __FILE__, __LINE__);
			}
		}
		else
		{
			if ( !ref_path.empty() )		// 非绝对路径
			{
				str_path += ref_path;

				if ( !TryMakeDir(str_path) )
				{
					throw Exception(LG_FILE_PATH_INVALID, "[LOG] The log path is invalid: %s [FILE:%s, LINE:%d]", path.c_str(), __FILE__, __LINE__);
				}
			}
		}
	}

	m_sLogPath = str_path + "/";
}

bool Log::Output(const char* format, ...)
{
	if ( !m_fsLogger.is_open() || !m_fsLogger.good() )
	{
		return false;
	}

	char buf[MAX_BUFFER_SIZE] = "";
	va_list arg_ptr;
	va_start(arg_ptr, format);
	vsnprintf(buf, sizeof(buf), format, arg_ptr);
	va_end(arg_ptr);

	std::string str_out = SimpleTime::Now().TimeStamp() + std::string("\x20\x20\x20\x20") + buf + std::string("\n");
	m_fsLogger.write(str_out.c_str(), str_out.size());
	m_fsLogger.flush();

	m_sCurrentFileSize += str_out.size();
	// Maximum file size
	if ( m_sCurrentFileSize >= _sMaxLogFileSize )
	{
		m_sCurrentFileSize = 0;

		str_out = "\n\n\x20\x20\x20\x20( MAXIMUM FILE SIZE )\n";
		m_fsLogger.write(str_out.c_str(), str_out.size());
		m_fsLogger.flush();
		m_fsLogger.close();

		OpenNewLogger();
	}
	return true;
}

void Log::TryCloseFileLogger()
{
	if ( m_fsLogger.is_open() )
	{
		m_fsLogger.flush();
		m_fsLogger.close();
	}
}

void Log::OpenNewLogger() throw(Exception)
{
	int log_id = 0;
	std::string fullLogPath;

	const std::string DAY_TIME = SimpleTime::Now().DayTime8();

	do
	{
		PubStr::SetFormatString(fullLogPath, "%sLOG_%lld_%s_%04d.log", m_sLogPath.c_str(), _sLogCcmID, DAY_TIME.c_str(), log_id++);

	} while ( access(fullLogPath.c_str(), 0) == 0 );

	m_fsLogger.open(fullLogPath.c_str(), std::fstream::out);
	if ( !m_fsLogger.is_open() || !m_fsLogger.good() )
	{
		throw Exception(LG_OPEN_FILE_FAIL, "[LOG] Can not open log file: %s [FILE:%s, LINE:%d]", fullLogPath.c_str(), __FILE__, __LINE__);
	}
}

}	// namespace base

