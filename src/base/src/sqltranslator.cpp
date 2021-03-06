#include "sqltranslator.h"
#include "pubstr.h"
#include "simpletime.h"

namespace base
{

const char* const SQLTranslator::S_ETL_DAY = "ETL_DAY";				// 采集时间：day
const char* const SQLTranslator::S_ETL_MON = "ETL_MON";				// 采集时间：month
const char* const SQLTranslator::S_SYS_DAY = "SYS_DAY";				// 系统（当前）时间：day
const char* const SQLTranslator::S_SYS_MON = "SYS_MON";				// 系统（当前）时间：month

const char* const SQLTranslator::S_FIRST_DAY_THIS_PM = "FIRST_DAY_THIS_PM";		// 本月1日
const char* const SQLTranslator::S_LAST_DAY_THIS_PM  = "LAST_DAY_THIS_PM";		// 本账期月最后一天
const char* const SQLTranslator::S_LAST_DAY_LAST_PM  = "LAST_DAY_LAST_PM";		// 上个月最后一天

SQLTranslator::SQLTranslator(PubTime::DATE_TYPE dt, const std::string& etl_date)
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

bool SQLTranslator::GetMark(const std::string& src, std::string& mark, size_t& pos)
{
	size_t pos_mark_beg = src.find("$(");
	if ( pos_mark_beg != std::string::npos )
	{
		size_t pos_mark_end = src.find(")", pos_mark_beg+2);
		// Not find a pair "$(...)"
		if ( std::string::npos == pos_mark_end )
		{
			return false;
		}

		pos  = pos_mark_beg;
		mark = src.substr(pos_mark_beg+2, pos_mark_end-pos_mark_beg-2);
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
	PubStr::Str2StrVector(mark, OPER, vec_str);

	if ( vec_str.size() != 2 )
	{
		if ( pError != NULL )
		{
			PubStr::SetFormatString(*pError, "[SQLTranslator] Not support mark: %s [FILE:%s, LINE:%d]", mark.c_str(), __FILE__, __LINE__);
		}
		return false;
	}

	if ( !TransMark(vec_str[0], val, pError) )
	{
		return false;
	}

	unsigned int u_off = 0;
	if ( !PubStr::Str2UInt(vec_str[1], u_off) )
	{
		if ( pError != NULL )
		{
			PubStr::SetFormatString(*pError, "[SQLTranslator] Not support mark: %s [FILE:%s, LINE:%d]", mark.c_str(), __FILE__, __LINE__);
		}
		return false;
	}

	int year = 0;
	int mon  = 0;
	int day  = 0;

	const int VAL_SIZE = val.size();
	if ( VAL_SIZE == 8 )	// day
	{
		PubStr::Str2Int(val.substr(0, 4), year);
		PubStr::Str2Int(val.substr(4, 2), mon);
		PubStr::Str2Int(val.substr(6, 2), day);

		if ( is_plus )
		{
			val = PubTime::TheDatePlusDays(year, mon, day, u_off);
		}
		else
		{
			val = PubTime::TheDateMinusDays(year, mon, day, u_off);
		}
	}
	else if ( VAL_SIZE == 6 )	// month
	{
		PubStr::Str2Int(val.substr(0, 4), year);
		PubStr::Str2Int(val.substr(4, 2), mon);

		if ( is_plus )
		{
			val = PubTime::TheDatePlusMonths(year, mon, u_off);
		}
		else
		{
			val = PubTime::TheDateMinusMonths(year, mon, u_off);
		}
	}
	else
	{
		if ( pError != NULL )
		{
			PubStr::SetFormatString(*pError, "[SQLTranslator] Can not calculate the date: %s [FILE:%s, LINE:%d]", val.c_str(), __FILE__, __LINE__);
		}
		return false;
	}

	return true;
}

bool SQLTranslator::TransMark(const std::string& mark, std::string& val, std::string* pError)
{
	const std::string T_MARK = PubStr::TrimUpperB(mark);
	if ( T_MARK == S_ETL_DAY )
	{
		if ( m_dEtlType != PubTime::DT_DAY && m_dEtlType != PubTime::DT_MONTH )
		{
			if ( pError != NULL )
			{
				PubStr::SetFormatString(*pError, "[SQLTranslator] Unknown DATE_TYPE for [%s]: %d [FILE:%s, LINE:%d]", T_MARK.c_str(), m_dEtlType, __FILE__, __LINE__);
			}
			return false;
		}

		val = m_sEtlDate;
	}
	else if ( T_MARK == S_ETL_MON )
	{
		if ( PubTime::DT_DAY == m_dEtlType )
		{
			val = m_sEtlDate.substr(0, 6);
		}
		else if ( PubTime::DT_MONTH == m_dEtlType )
		{
			val = m_sEtlDate;
		}
		else
		{
			if ( pError != NULL )
			{
				PubStr::SetFormatString(*pError, "[SQLTranslator] Unknown DATE_TYPE for [%s]: %d [FILE:%s, LINE:%d]", T_MARK.c_str(), m_dEtlType, __FILE__, __LINE__);
			}
			return false;
		}
	}
	else if ( T_MARK == S_SYS_DAY )
	{
		val = SimpleTime::Now().DayTime8();
	}
	else if ( T_MARK == S_SYS_MON )
	{
		val = SimpleTime::Now().MonTime6();
	}
	else if ( T_MARK == S_FIRST_DAY_THIS_PM )
	{
		val = m_sEtlDate.substr(0, 6) + "01";
	}
	else if ( T_MARK == S_LAST_DAY_THIS_PM )
	{
		int year = 0;
		int mon  = 0;
		PubStr::Str2Int(m_sEtlDate.substr(0, 4), year);
		PubStr::Str2Int(m_sEtlDate.substr(4, 2), mon);

		PubStr::SetFormatString(val, "%04d%02d%02d", year, mon, SimpleTime::LastDayOfTheMon(year, mon));
	}
	else if ( T_MARK == S_LAST_DAY_LAST_PM )
	{
		int year = 0;
		int mon  = 0;
		PubStr::Str2Int(m_sEtlDate.substr(0, 4), year);
		PubStr::Str2Int(m_sEtlDate.substr(4, 2), mon);

		// 上个月
		if ( mon > 1 )		// 非一月份
		{
			--mon;
		}
		else	// 一月份
		{
			--year;
			mon = 12;
		}

		PubStr::SetFormatString(val, "%04d%02d%02d", year, mon, SimpleTime::LastDayOfTheMon(year, mon));
	}
	else
	{
		if ( pError != NULL )
		{
			PubStr::SetFormatString(*pError, "[SQLTranslator] Not support mark: %s [FILE:%s, LINE:%d]", T_MARK.c_str(), __FILE__, __LINE__);
		}
		return false;
	}

	return true;
}

}	// namespace base

