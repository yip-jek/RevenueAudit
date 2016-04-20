#pragma once

#include <stdio.h>
#include <string>
#include <stdarg.h>

namespace base
{

class Exception
{
public:
	Exception():m_errorcode(-1)
	{}
	Exception(int error, const std::string& descript):m_errorcode(error), m_descript(descript)
	{}
	Exception(int error, const char* format, ...): m_errorcode(error)
	{
		char buf[2048] = "";

		va_list ap;
		va_start(ap, format);
		vsnprintf(buf, sizeof(buf)-1, format, ap);
		va_end(ap);

		m_descript = buf;
	}

	virtual ~Exception() {}

public:
	void ErrorCode(int error)
	{ m_errorcode = error; }

	int ErrorCode() const
	{ return m_errorcode; }

	std::string What() const
	{ return m_descript; }

	void Descript(const char* format, ...)
	{
		char buf[2048] = "";

		va_list ap;
		va_start(ap, format);
		vsnprintf(buf, sizeof(buf)-1, format, ap);
		va_end(ap);

		m_descript = buf;

	}

protected:
	int 		m_errorcode;
	std::string	m_descript;
};

}	// namespace base

