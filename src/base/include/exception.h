#pragma once

#include <stdio.h>
#include <string>
#include <stdarg.h>

namespace base
{

class Exception
{
public:
	static const int MAX_EXP_BUF_SIZE = 4096;

public:
	Exception():m_errorcode(-1)
	{}
	Exception(int error, const std::string& descript):m_errorcode(error), m_descript(descript)
	{}
	Exception(int error, const char* format, ...): m_errorcode(error)
	{
		char buf[MAX_EXP_BUF_SIZE] = "";

		va_list ap;
		va_start(ap, format);
		vsnprintf(buf, sizeof(buf), format, ap);
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
		char buf[MAX_EXP_BUF_SIZE] = "";

		va_list ap;
		va_start(ap, format);
		vsnprintf(buf, sizeof(buf), format, ap);
		va_end(ap);

		m_descript = buf;

	}

protected:
	int 		m_errorcode;
	std::string	m_descript;
};

}	// namespace base

