#pragma once

#include "exception.h"

class YCTaskException : public base::Exception
{
public:
	YCTaskException(int error, const std::string& status, const std::string& desc, const std::string& err_descript): base::Exception(error, err_descript)
	,m_status(status)
	,m_statusdesc(desc)
	{}

	YCTaskException(int error, const std::string& status, const std::string& desc, const std::string& format, ...): m_status(status)
	,m_statusdesc(desc)
	{
		m_errorcode = error;

		char buf[MAX_EXP_BUF_SIZE] = "";

		va_list ap;
		va_start(ap, format);
		vsnprintf(buf, sizeof(buf), format, ap);
		va_end(ap);

		m_descript = buf;
	}

public:
	void SetStatus(const std::string& status)   { m_status     = status; }
	void SetStatusDesc(const std::string& desc) { m_statusdesc = desc;   }

	std::string GetStatus()     const { return m_status;     }
	std::string GetStatusDesc() const { return m_statusdesc; }

private:
	std::string m_status;				// 状态
	std::string m_statusdesc;			// 状态描述
};

