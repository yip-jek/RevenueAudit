#include "sqlextendconverter.h"

const char* const SQLExtendConverter::S_NO_NEED_EXTEND  = "[NO_NEED_EXTEND]";			// 不进行SQL扩展的标记
const char* const SQLExtendConverter::S_CITY_MARK       = "[CITY_MARK]";				// 地市标记（编码）
const char* const SQLExtendConverter::S_CN_CITY_MARK    = "[CN_CITY_MARK]";				// 地市标记（中文名称）

SQLExtendConverter::SQLExtendConverter(const std::string& city, const std::string& cn_city)
:m_city(city)
,m_cityCN(cn_city)
{
}

SQLExtendConverter::~SQLExtendConverter()
{
}

std::string SQLExtendConverter::Extend(const std::string& sql)
{
}

void SQLExtendConverter::CityConvert(std::string& sql)
{
}

