#include "sqlextendconverter.h"
#include "pubstr.h"
#include "log.h"
#include "acqerror.h"

const char SQLExtendConverter::S_SPACE             = '\x20';				// 空格
const char SQLExtendConverter::S_LEFT_PARENTHESIS  = '(';					// 左括号
const char SQLExtendConverter::S_RIGHT_PARENTHESIS = ')';					// 右括号

const char* const SQLExtendConverter::S_NO_NEED_EXTEND  = "[NO_NEED_EXTEND]";			// 不进行SQL扩展的标记
const char* const SQLExtendConverter::S_CITY_MARK       = "[CITY_MARK]";				// 地市标记（编码）
const char* const SQLExtendConverter::S_CN_CITY_MARK    = "[CN_CITY_MARK]";				// 地市标记（中文名称）

SQLExtendConverter::SQLExtendConverter(const SQLExtendData& sqlex_data)
:m_pLog(base::Log::Instance())
,m_sqlExData(sqlex_data)
{
	Init();
}

SQLExtendConverter::~SQLExtendConverter()
{
	base::Log::Release();
}

void SQLExtendConverter::Extend(std::string& sql)
{
	base::PubStr::Trim(sql);

	// 不需要进行SQL扩展？
	if ( !NoNeedExtend(sql) )
	{
		DoExtend(sql);
	}
}

void SQLExtendConverter::CityConvert(std::string& sql)
{
	const std::string CITY_MARK    = S_CITY_MARK;
	const std::string CN_CITY_MARK = S_CN_CITY_MARK;
	const size_t      CITY_SIZE    = CITY_MARK.size();
	const size_t      CN_CITY_SIZE = CN_CITY_MARK.size();

	size_t      pos_find  = 0;
	std::string upper_sql = base::PubStr::UpperB(sql);

	// 地市编码转换
	while ( (pos_find = upper_sql.find(CITY_MARK)) != std::string::npos )
	{
		sql.replace(pos_find, CITY_SIZE, m_sqlExData.city);
		upper_sql = base::PubStr::UpperB(sql);
	}

	// 地市中文名称转换
	while ( (pos_find = upper_sql.find(CN_CITY_MARK)) != std::string::npos )
	{
		sql.replace(pos_find, CN_CITY_SIZE, m_sqlExData.cityCN);
		upper_sql = base::PubStr::UpperB(sql);
	}
}

void SQLExtendConverter::Init()
{
	base::PubStr::SetFormatString(m_subCond, "%s = '%s' and %s = '%s'", 
								m_sqlExData.field_period.c_str(), 
								m_sqlExData.period.c_str(), 
								m_sqlExData.field_city.c_str(), 
								m_sqlExData.city.c_str());

	base::PubStr::SetFormatString(m_extendCond, "%s and decimal(%s,12,2) in (select max(decimal(%s,12,2)) from ", 
								m_subCond.c_str(), 
								m_sqlExData.field_batch.c_str(), 
								m_sqlExData.field_batch.c_str());
}

bool SQLExtendConverter::NoNeedExtend(std::string& sql)
{
	const std::string NO_NEED_EXTEND = S_NO_NEED_EXTEND;
	const size_t NO_NEED_EXTEND_SIZE = NO_NEED_EXTEND.size();

	if ( sql.size() >= NO_NEED_EXTEND_SIZE )
	{
		const std::string SQL_HEAD = base::PubStr::UpperB(sql.substr(0, NO_NEED_EXTEND_SIZE));
		if ( SQL_HEAD == NO_NEED_EXTEND )
		{
			// 删除标记：NO_NEED_EXTEND
			sql.erase(0, NO_NEED_EXTEND_SIZE);
			return true;
		}
	}

	return false;
}

void SQLExtendConverter::DoExtend(std::string& sql) throw(base::Exception)
{
	const std::string MARK_FROM  = " FROM ";
	const std::string MARK_WHERE = "WHERE ";
	const size_t MARK_FROM_SIZE  = MARK_FROM.size();
	const size_t MARK_WHERE_SIZE = MARK_WHERE.size();

	size_t beg_pos = 0;
	size_t end_pos = 0;
	size_t pos_off = 0;

	std::string tab_name;
	std::string ins_cond;

	const std::string C_SQL = base::PubStr::UpperB(sql);
	while ( (beg_pos = C_SQL.find(MARK_FROM, beg_pos)) != std::string::npos )
	{
		beg_pos += MARK_FROM_SIZE;

		// Skip spaces, find the beginning of table name
		beg_pos = C_SQL.find_first_not_of(S_SPACE, beg_pos);
		if ( std::string::npos == beg_pos )		// All spaces
		{
			throw base::Exception(ACQERR_SQL_EXTEND_CONV_FAILED, "统计因子 SQL 扩展失败：Table name not found in position [%lu] [FILE:%s, LINE:%d]", beg_pos, __FILE__, __LINE__);
		}

		// Find the end of table name
		end_pos = C_SQL.find_first_of(S_SPACE, beg_pos);
		if ( std::string::npos == end_pos )		// No space
		{
			tab_name = C_SQL.substr(beg_pos);
			if ( S_LEFT_PARENTHESIS == tab_name[0] )		// 表名前带左括号
			{
				throw base::Exception(ACQERR_SQL_EXTEND_CONV_FAILED, "统计因子 SQL 扩展失败：Invalid table name in position [%lu] [FILE:%s, LINE:%d]", beg_pos, __FILE__, __LINE__);
			}

			end_pos = beg_pos + pos_off + tab_name.size();
			if ( !DealWithRightParenthes(sql, tab_name, end_pos, pos_off) )
			{
				base::PubStr::SetFormatString(ins_cond, " where %s %s where %s)", 
														m_extendCond.c_str(), 
														tab_name.c_str(), 
														m_subCond.c_str());
				sql.insert(end_pos, ins_cond);
			}
			break;
		}
		else
		{
			tab_name = C_SQL.substr(beg_pos, end_pos-beg_pos);
			if ( S_LEFT_PARENTHESIS == tab_name[0] )		// Skip: 非表名
			{
				beg_pos = end_pos;
				continue;
			}

			if ( DealWithRightParenthes(sql, tab_name, end_pos, pos_off) )
			{
				beg_pos = end_pos;
				continue;
			}

			end_pos = C_SQL.find_first_not_of(S_SPACE, end_pos);
			if ( std::string::npos == end_pos )		// All spaces
			{
				base::PubStr::SetFormatString(ins_cond, "where %s %s where %s)", 
														m_extendCond.c_str(), 
														tab_name.c_str(), 
														m_subCond.c_str());
				sql += ins_cond;
				break;
			}
			else if ( C_SQL.substr(end_pos, MARK_WHERE_SIZE) == MARK_WHERE )	// 表名后带"WHERE"
			{
				base::PubStr::SetFormatString(ins_cond, "%s %s where %s) and ", 
														m_extendCond.c_str(), 
														tab_name.c_str(), 
														m_subCond.c_str());
				end_pos += MARK_WHERE_SIZE;
			}
			else	// 表名后不带"WHERE"
			{
				base::PubStr::SetFormatString(ins_cond, "where %s %s where %s) ", 
														m_extendCond.c_str(), 
														tab_name.c_str(), 
														m_subCond.c_str());
			}

			sql.insert(end_pos+pos_off, ins_cond);
			pos_off += ins_cond.size();
			beg_pos = end_pos;
		}
	}
}

bool SQLExtendConverter::DealWithRightParenthes(std::string& sql, const std::string& tab, size_t& end, size_t& off)
{
	const size_t RP_SIZE = tab.size() - tab.find_last_not_of(S_RIGHT_PARENTHESIS) - 1;
	if ( RP_SIZE > 0 )		// 表名后带右括号
	{
		std::string ins_sub;
		base::PubStr::SetFormatString(ins_sub, " where %s %s where %s)", 
												m_extendCond.c_str(), 
												tab.substr(0, tab.size()-RP_SIZE).c_str(), 
												m_subCond.c_str());

		end -= RP_SIZE;
		sql.insert(end, ins_sub);

		off += ins_sub.size();
		return true;
	}

	return false;
}

