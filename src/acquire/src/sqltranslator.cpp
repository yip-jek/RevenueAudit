#include "sqltranslator.h"
#include "pubstr.h"
#include "simpletime.h"

const char* const SQLTranslator::S_ETL_DAY = "ETL_DAY";				// 采集时间：day
const char* const SQLTranslator::S_ETL_MON = "ETL_MON";				// 采集时间：month
const char* const SQLTranslator::S_SYS_DAY = "SYS_DAY";				// 系统（当前）时间：day
const char* const SQLTranslator::S_SYS_MON = "SYS_MON";				// 系统（当前）时间：month

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

	while ( GetMark(sql, mark, pos) )
	{
		// 整个 $(...) 的大小
		mark_size = mark.size() + 3;

		if ( mark.find('+') != std::string::npos )			// 含 + 运算
		{
			if ( !CalcMark(mark, true, mark_val, pError) )
			{
				return false;
			}
		}
		else if ( mark.find('-') != std::string::npos )		// 含 - 运算
		{
			if ( !CalcMark(mark, false, mark_val, pError) )
			{
				return false;
			}
		}
		else	// 不含 + 或者 - 运算
		{
			if ( !TransMark(mark, mark_val, pError) )
			{
				return false;
			}
		}

		sql.replace(pos, mark_size, mark_val);
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

bool SQLTranslator::CalcMark(const std::string& mark, bool is_plus, std::string& val, std::string* pError)
{
	std::vector<std::string> vec_str;
	const std::string OPER = (is_plus ? "+" : "-");
	base::PubStr::Str2StrVector(mark, OPER, vec_str);

	if ( vec_str.size() != 2 )
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[SQLTranslator] Not support mark: %s [FILE:%s, LINE:%d]", mark.c_str(), __FILE__, __LINE__);
		}
		return false;
	}

	if ( !TransMark(vec_str[0], val, pError) )
	{
		return false;
	}

	unsigned int u_off = 0;
	if ( !base::PubStr::Str2UInt(vec_str[1], u_off) )
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[SQLTranslator] Not support mark: %s [FILE:%s, LINE:%d]", mark.c_str(), __FILE__, __LINE__);
		}
		return false;
	}

	int year = 0;
	int mon  = 0;
	int day  = 0;

	const int VAL_SIZE = val.size();
	if ( VAL_SIZE == 8 )	// day
	{
		base::PubStr::Str2Int(val.substr(0, 4), year);
		base::PubStr::Str2Int(val.substr(4, 2), mon);
		base::PubStr::Str2Int(val.substr(6, 2), day);

		if ( is_plus )
		{
			val = base::PubTime::TheDatePlusDays(year, mon, day, u_off);
		}
		else
		{
			val = base::PubTime::TheDateMinusDays(year, mon, day, u_off);
		}
	}
	else if ( VAL_SIZE == 6 )	// month
	{
		base::PubStr::Str2Int(val.substr(0, 4), year);
		base::PubStr::Str2Int(val.substr(4, 2), mon);

		if ( is_plus )
		{
			val = base::PubTime::TheDatePlusMonths(year, mon, u_off);
		}
		else
		{
			val = base::PubTime::TheDateMinusMonths(year, mon, u_off);
		}
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[SQLTranslator] Can not calculate the date: %s [FILE:%s, LINE:%d]", val.c_str(), __FILE__, __LINE__);
		}
		return false;
	}

	return true;
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
				base::PubStr::SetFormatString(*pError, "[SQLTranslator] Unknown DATE_TYPE for [%s]: %d [FILE:%s, LINE:%d]", T_MARK.c_str(), m_dEtlType, __FILE__, __LINE__);
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
			if ( pError != NULL )
			{
				base::PubStr::SetFormatString(*pError, "[SQLTranslator] Unknown DATE_TYPE for [%s]: %d [FILE:%s, LINE:%d]", T_MARK.c_str(), m_dEtlType, __FILE__, __LINE__);
			}
			return false;
		}
	}
	else if ( T_MARK == S_SYS_DAY )
	{
		val = base::SimpleTime::Now().DayTime8();
	}
	else if ( T_MARK == S_SYS_MON )
	{
		val = base::SimpleTime::Now().MonTime6();
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[SQLTranslator] Not support mark: %s [FILE:%s, LINE:%d]", T_MARK.c_str(), __FILE__, __LINE__);
		}
		return false;
	}

	return true;
}

