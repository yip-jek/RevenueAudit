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

	typedef std::map<std::string, DimVal>		MAP_VAL;			// 这里的key值为维度取值
	typedef std::map<std::string, MAP_VAL>		MAP_DIM_VAL;		// 这里的key值为维度名称

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
	size_t GetDBDimValSize();

	// 获得源数据的维度取值集size
	size_t GetSrcDimValSize();

private:
	// 获取维度取值
	void FetchDimVal(MAP_DIM_VAL& mm_dv, const DimVal& dv);

	// 获取数据集的size
	size_t GetMapDimValSize(MAP_DIM_VAL& mdv);

private:
	MAP_DIM_VAL	mmSrcDimVal;		// 源数据的维度取值集
	MAP_DIM_VAL	mmDBDimVal;			// 数据库表的维度取值集
};

