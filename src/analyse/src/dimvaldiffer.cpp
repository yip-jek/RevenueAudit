#include "dimvaldiffer.h"
#include <boost/algorithm/string.hpp>


DimValDiffer::DimValDiffer()
{
}

DimValDiffer::~DimValDiffer()
{
}

void DimValDiffer::FetchSrcDimVal(const DimVal& dv)
{
	FetchDimVal(m_mSrcDimVal, dv);
}

void DimValDiffer::FetchDBDimVal(const DimVal& dv)
{
	FetchDimVal(m_mDBDimVal, dv);
}

void DimValDiffer::GetDimValDiff(std::vector<DimVal>& vec_dv)
{
	std::vector<DimVal> v_dv;

	for ( std::map<std::string, DimVal>::iterator it = m_mSrcDimVal.begin(); it != m_mSrcDimVal.end(); ++it )
	{
		// 存在于源数据中，但不存在于数据库中
		if ( m_mDBDimVal.find(it->first) == m_mDBDimVal.end() )
		{
			v_dv.push_back(it->second);
		}
	}

	v_dv.swap(vec_dv);
}

size_t DimValDiffer::GetDBDimValSize() const
{
	return m_mDBDimVal.size();
}

size_t DimValDiffer::GetSrcDimValSize() const
{
	return m_mSrcDimVal.size();
}

void DimValDiffer::FetchDimVal(std::map<std::string, DimVal>& m_dv, const DimVal& dv)
{
	std::string name  = dv.DBName;
	std::string value = dv.Value;

	boost::trim(name);
	boost::trim(value);

	// 字段名称统一转成大写
	boost::to_upper(name);

	// 只添加新的维度取值
	const std::string KEY = name + value;
	if ( m_dv.find(KEY) == m_dv.end() )
	{
		m_dv[KEY] = dv;
	}
}

