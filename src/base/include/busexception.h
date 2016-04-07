#pragma once

#include "exception.h"

class BusException : public Exception
{
public:
	BusException() {}
	BusException(int error, const std::string& descript): Exception(error, descript)
	{}
	BusException(int error, const char* fmt, ...)
	{
		m_errorcode = error;

		char buf[2048] = "";

		va_list ap;
		va_start(ap, fmt);
		vsnprintf(buf, sizeof(buf)-1, fmt, ap);
		va_end(ap);

		m_descript = buf;
	}
	virtual ~BusException();

public:
	void SetErrorInfo(int error, const char* fmt, ...)
	{
		m_errorcode = error;

		char buf[2048] = "";

		va_list ap;
		va_start(ap, fmt);
		vsnprintf(buf, sizeof(buf)-1, fmt, ap);
		va_end(ap);

		m_descript = buf;
	}

	std::string GetErrorInfo() const
	{
		return ("[DSUException] "+m_descript+" ERROR_CODE: "+PubStr::LLong2Str(m_errorcode));
	}
};

