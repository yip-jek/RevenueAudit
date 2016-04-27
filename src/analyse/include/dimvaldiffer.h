#pragma once

#include <map>
#include <vector>
#include "anataskinfo.h"

// 维度取值差异比较
class DimValDiffer
{
private:	// noncopyable
	DimValDiffer(const DimValDiffer& );
	const DimValDiffer& operator = (const DimValDiffer& );

public:
	DimValDiffer();
	~DimValDiffer();

public:
	// 添加源数据的维度取值
	void FetchSrcDimVal(const DimVal& dv);

	// 添加数据库表的维度取值
	void FetchDBDimVal(const DimVal& dv);

	// 获得维度取值的差异集: 存在于源数据中，但数据库中不存在
	void GetDimValDiff(std::vector<DimVal>& vec_dv);

	// 获得数据库表的维度取值集size
	size_t GetDBDimValSize() const;

	// 获得源数据的维度取值集size
	size_t GetSrcDimValSize() const;

private:
	// 获取维度取值
	void FetchDimVal(std::map<std::string, DimVal>& m_dv, const DimVal& dv);

private:
	std::map<std::string, DimVal>	m_mSrcDimVal;			// 源数据的维度取值集: key值为[字段名称]+[维度取值]
	std::map<std::string, DimVal>	m_mDBDimVal;			// 数据库表的维度取值集: key值为[字段名称]+[维度取值]
};

