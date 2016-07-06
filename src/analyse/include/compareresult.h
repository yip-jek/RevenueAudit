#pragma once

#include <map>
#include <string>
#include <vector>
#include "exception.h"

// 对比数据
struct CompareData
{
public:
	CompareData(): dim_size(0)
	{}

	typedef std::map<std::string, std::vector<std::string> >	MAP_DATA;

public:
	MAP_DATA	map_comdata;		// 数据清单
	int			dim_size;			// 维度个数
};

class CompareResult;

// 对比数据索引
class ComDataIndex
{
	friend class CompareResult;

public:
	ComDataIndex(): m_dataIndex(-1)
	{}

private:
	bool operator == (const ComDataIndex& cd_index) const
	{ return (this->m_dataIndex == cd_index.m_dataIndex); }

private:
	// 索引是否于当前对比数据组有效
	bool IsValid(std::vector<CompareData>& vec_comdata) const
	{ return (m_dataIndex >= 0 && m_dataIndex < (int)vec_comdata.size()); }

	// 指向组内的对比数据
	// 前提：该索引有效
	CompareData* At(std::vector<CompareData>& vec_comdata) const
	{ return (IsValid(vec_comdata) ? &vec_comdata[m_dataIndex] : NULL); }

private:
	int m_dataIndex;
};

// 数据对比结果类
class CompareResult
{
public:
	CompareResult();
	virtual ~CompareResult();

public:
	enum COM_RES_ERROR
	{
		CRERR_SET_COMPARE_DATA   = -3005001,		// 设置对比数据失败
		CRERR_GET_COMPARE_RESULT = -3005002,		// 获取对比结果数据失败
	};

	enum COMPARE_TYPE
	{
		CTYPE_EQUAL	= 1,		// 对平结果
		CTYPE_DIFF	= 2,		// 有差异结果
		CTYPE_LEFT	= 3,		// 左有右无结果
		CTYPE_RIGHT	= 4,		// 左无右有结果
	};

public:
	// 设置对比数据，返回数据索引
	ComDataIndex SetCompareData(std::vector<std::vector<std::string> >& vec2_data, int size_dim);

	// 获取对比结果数据
	void GetCompareResult(const ComDataIndex& left_index, const ComDataIndex& right_index, COMPARE_TYPE com_type, 
		std::string& result_desc, std::vector<std::vector<std::string> >& vec2_result) throw(base::Exception);

private:

private:
	std::vector<CompareData>	m_vComData;
};

