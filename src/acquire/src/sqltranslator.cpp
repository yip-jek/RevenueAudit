#include "sqltranslator.h"
#include "pubstr.h"
#include "simpletime.h"

const char* const SQLTranslator::S_ETL_DAY = "ETL_DAY";				// 采集时间：天
const char* const SQLTranslator::S_ETL_MON = "ETL_MON";				// 采集时间：月
const char* const SQLTranslator::S_SYS_DAY = "SYS_DAY";				// 系统（当前）时间：天
const char* const SQLTranslator::S_SYS_MON = "SYS_MON";				// 系统（当前）时间：月

SQLTranslator::SQLTranslator(base::PubTime::DATE_TYPE dt, const std::string& etl_date)
:m_dEtlType(dt), m_sEtlDate(etl_date)
{
}

SQLTranslator::~SQLTranslator()
{
}

bool SQLTranslator::Translate(std::string& sql, std::string* pError)
{
	size_t pos       = 0;
	size_t mark_size = 0;
	std::string mark;
	std::string mark_val;
	std::vector<std::string> vec_str;

	while ( GetMark(sql, mark, pos) )
	{
		// 整个 $(...) 的大小
		mark_size = mark.size() + 3;

		if ( mark.find('+') != std::string::npos )			// 含 + 运算
		{
		}
		else if ( mark.find('-') != std::string::npos )		// 含 - 运算
		{
		}
		else	// 不含 + 或者 - 运算
		{
			if ( !TransMark(mark, mark_val, pError) )
			{
				return false;
			}
		}
	}

	return true;
}

bool SQLTranslator::GetMark(const std::string& sql, std::string& mark, size_t& pos)
{
	size_t pos_mark_beg = sql.find("$(");
	if ( pos_mark_beg != std::string::npos )
	{
		size_t pos_mark_end = sql.find(")", pos_mark_beg+2);
		// Not find a pair "$(...)"
		if ( std::string::npos == pos_mark_end )
		{
			return false;
		}

		pos  = pos_mark_beg;
		mark = sql.substr(pos_mark_beg+2, pos_mark_end-pos_mark_beg-2);
		return true;
	}
	else	// Not find "$("
	{
		return false;
	}
}

bool SQLTranslator::TransMark(const std::string& mark, std::string& val, std::string* pError)
{
	const std::string T_MARK = base::PubStr::TrimUpperB(mark);
	if ( T_MARK == S_ETL_DAY )
	{
		if ( m_dEtlType != base::PubTime::DT_DAY && m_dEtlType != base::PubTime::DT_MONTH )
		{
			if ( pError != NULL )
			{
				;
			}
			return false;
		}

		val = m_sEtlDate;
	}
	else if ( T_MARK == S_ETL_MON )
	{
		if ( base::PubTime::DT_DAY == m_dEtlType )
		{
			val = m_sEtlDate.substr(0, 6);
		}
		else if ( base::PubTime::DT_MONTH == m_dEtlType )
		{
			val = m_sEtlDate;
		}
		else
		{
			return false;
		}
	}
	else if ( T_MARK == S_SYS_DAY )
	{
		val = base::SimpleTime::DayTime8();
	}
	else if ( T_MARK == S_SYS_MON )
	{
		val = base::SimpleTime::MonTime6();
	}
	else
	{
		return false;
	}

	return true;
}

