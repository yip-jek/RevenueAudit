#pragma once

#include <string>
#include <fstream>
#include "exception.h"

namespace base
{

// Log error code
#define LE_CCMID_INVALID              (-100001)
#define LE_FILE_PATH_EMPTY            (-100002)
#define LE_FILE_PATH_INVALID          (-100003)
#define LE_INIT_FAIL                  (-100004)
#define LE_OPEN_FILE_FAIL             (-100005)

class Log
{
private:
	Log();
	~Log();

public:
	static Log* Instance();
	static void Release();
	static bool SetCCMID(long long ccm_id);
	static bool ResetFileSize(unsigned long fsize);

public:
	void Init() throw(Exception);
	void SetPath(const std::string& path) throw(Exception);
	bool Output(const char* format, ...);

private:
	void TryCloseFileLogger();
	void OpenNewLogger() throw(Exception);

private:
	static Log*          _spLogger;
	static long long     _sLogCcmID;
	static unsigned long _sMaxLogFileSize;

private:
	std::string    m_sLogPath;
	std::fstream   m_fsLogger;
	unsigned long  m_sCurrentFileSize;
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

