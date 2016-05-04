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
	FetchDimVal(mmSrcDimVal, dv);
}

void DimValDiffer::FetchDBDimVal(const DimVal& dv)
{
	FetchDimVal(mmDBDimVal, dv);
}

void DimValDiffer::GetDimValDiff(std::vector<DimVal>& vec_dv)
{
	std::vector<DimVal> v_dv;

	for ( MAP_DIM_VAL::iterator src_mdv_it = mmSrcDimVal.begin(); src_mdv_it != mmSrcDimVal.end(); ++src_mdv_it )
	{
		MAP_VAL& ref_src_mv = src_mdv_it->second;

		MAP_DIM_VAL::iterator db_mdv_it = mmDBDimVal.find(src_mdv_it->first);
		if ( db_mdv_it != mmDBDimVal.end() )	// 维度名称已存在
		{
			MAP_VAL& ref_db_mv = db_mdv_it->second;

			for ( MAP_VAL::iterator mv_it = ref_src_mv.begin(); mv_it != ref_src_mv.end(); ++mv_it )
			{
				// 存在于源数据中，但不存在于数据库中
				if ( ref_db_mv.find(mv_it->first) == ref_db_mv.end() )
				{
					v_dv.push_back(mv_it->second);
				}
			}
		}
		else		// 维度名称不存在
		{
			// 全部插入
			for ( MAP_VAL::iterator mv_it = ref_src_mv.begin(); mv_it != ref_src_mv.end(); ++mv_it )
			{
				v_dv.push_back(mv_it->second);
			}
		}
	}

	v_dv.swap(vec_dv);
}

size_t DimValDiffer::GetDBDimValSize()
{
	return GetMapDimValSize(mmDBDimVal);
}

size_t DimValDiffer::GetSrcDimValSize()
{
	return GetMapDimValSize(mmSrcDimVal);
}

void DimValDiffer::FetchDimVal(MAP_DIM_VAL& mm_dv, const DimVal& dv)
{
	std::string name  = dv.DBName;
	std::string value = dv.Value;

	boost::trim(name);
	boost::trim(value);

	// 字段名称统一转成大写
	boost::to_upper(name);

	// 只添加新的维度取值
	MAP_DIM_VAL::iterator mdv_it = mm_dv.find(name);
	if ( mdv_it != mm_dv.end() )	// 该维度存在
	{
		MAP_VAL& ref_mv = mdv_it->second;
		if ( ref_mv.find(value) == ref_mv.end() )	// 该维度取值不存在
		{
			ref_mv[value] = dv;
		}
	}
	else		// 该维度不存在
	{
		// 新建一个MAP_VAL
		MAP_VAL m_v;
		m_v[value] = dv;

		mm_dv[name] = m_v;
	}
}

size_t DimValDiffer::GetMapDimValSize(MAP_DIM_VAL& mdv)
{
	size_t total_size = 0;
	for ( MAP_DIM_VAL::iterator it = mdv.begin(); it != mdv.end(); ++it )
	{
		total_size += it->second.size();
	}

	return total_size;
}

