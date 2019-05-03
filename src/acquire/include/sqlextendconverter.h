#pragma once

#include <string>
#include "exception.h"

namespace base
{
class Log;
}

// SQL 扩展数据
struct SQLExtendData
{
	std::string field_period;				// 字段名：账期
	std::string field_city;					// 字段名：地市
	std::string field_batch;				// 字段名：批次
	std::string period;						// 账期
	std::string city;						// 地市
	std::string cityCN;						// 地市中文名
};

// SQL 扩展转换
class SQLExtendConverter
{
public:
	static const char S_SPACE;							// 空格
	static const char S_LEFT_PARENTHESIS;				// 左括号
	static const char S_RIGHT_PARENTHESIS;				// 右括号

	static const char* const S_NO_NEED_EXTEND;			// 不进行SQL扩展的标记
	static const char* const S_CITY_MARK;				// 地市标记（编码）
	static const char* const S_CN_CITY_MARK;			// 地市标记（中文名称）

public:
	SQLExtendConverter(const SQLExtendData& sqlex_data);
	~SQLExtendConverter();

public:
	// SQL 扩展
	void Extend(std::string& sql);

	// 地市转换
	void CityConvert(std::string& sql);

private:
	// 初始化
	void Init();

	// 是否不需要进行 SQL 扩展
	bool NoNeedExtend(std::string& sql);

	// 进行 SQL 扩展
	void DoExtend(std::string& sql);

	// 处理表名后带右括号的情况：带右括号返回true，否则返回false
	bool DealWithRightParenthes(std::string& sql, const std::string& tab, size_t& end, size_t& off);

private:
	base::Log*    m_pLog;
	SQLExtendData m_sqlExData;
	std::string   m_subCond;
	std::string   m_extendCond;
};

