#pragma once

#include <string>

// SQL 扩展转换
class SQLExtendConverter
{
public:
	static const char* const S_NO_NEED_EXTEND;			// 不进行SQL扩展的标记
	static const char* const S_CITY_MARK;				// 地市标记（编码）
	static const char* const S_CN_CITY_MARK;			// 地市标记（中文名称）

public:
	SQLExtendConverter(const std::string& city, const std::string& cn_city);
	~SQLExtendConverter();

public:
	// SQL 扩展
	std::string Extend(const std::string& sql);

	// 地市转换
	void CityConvert(std::string& sql);

private:
	std::string m_city;					// 地市
	std::string m_cityCN;				// 地市中文名
};

