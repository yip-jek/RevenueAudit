#pragma once

#include "pubtime.h"

namespace base
{

// SQL 语句转换
class SQLTranslator
{
public:
	SQLTranslator(PubTime::DATE_TYPE dt, const std::string& etl_date);
	virtual ~SQLTranslator();

public:
	// 标记，格式：$(...)
	static const char* const S_ETL_DAY;				// 采集时间：day
	static const char* const S_ETL_MON;				// 采集时间：month
	static const char* const S_SYS_DAY;				// 系统（当前）时间：day
	static const char* const S_SYS_MON;				// 系统（当前）时间：month

	static const char* const S_FIRST_DAY_THIS_PM;		// 本账期月1日
	static const char* const S_LAST_DAY_THIS_PM;		// 本账期月最后一天
	static const char* const S_LAST_DAY_LAST_PM;		// 上个账期月最后一天

public:
	// 转换
	bool Translate(std::string& sql, std::string* pError);

private:
	// 获取标记
	bool GetMark(const std::string& sql, std::string& mark, size_t& pos);

	// 标志计算
	bool CalcMark(const std::string& mark, bool is_plus, std::string& val, std::string* pError);

	// 标志转换
	bool TransMark(const std::string& mark, std::string& val, std::string* pError);

private:
	PubTime::DATE_TYPE m_dEtlType;			// 时间类型
	std::string        m_sEtlDate;			// 采集时间（账期时间）
};

}	// namespace base

