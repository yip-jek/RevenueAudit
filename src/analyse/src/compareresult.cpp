#include "compareresult.h"
#include "pubstr.h"

CompareResult::CompareResult()
{
}

CompareResult::~CompareResult()
{
}

ComDataIndex CompareResult::SetCompareData(std::vector<std::vector<std::string> >& vec2_data, int size_dim)
{
	if ( vec2_data.empty() )
	{
		throw base::Exception(CRERR_SET_COMPARE_DATA, "NO compare data to be set! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( size_dim <= 0 )
	{
		throw base::Exception(CRERR_SET_COMPARE_DATA, "Invalid dim size: %d [FILE:%s, LINE:%d]", size_dim, __FILE__, __LINE__);
	}

	m_vComData.push_back(CompareData());
	int new_index = m_vComData.size() - 1;

	CompareData& ref_comdata = m_vComData[new_index];
	ref_comdata.dim_size     = size_dim;

	std::string str_key;
	const size_t VEC2_SIZE = vec2_data.size();

	for ( size_t i = 0; i < VEC2_SIZE; ++i )
	{
		std::vector<std::string>& ref_vec = vec2_data[i];

		const int REF_VSIZE = ref_vec.size();
		if ( REF_VSIZE < size_dim )
		{
			throw base::Exception(CRERR_SET_COMPARE_DATA, "[Index:%llu] Compare data size [%d] less than dim size [%d] ! [FILE:%s, LINE:%d]", (i+1), REF_VSIZE, size_dim, __FILE__, __LINE__);
		}

		str_key.clear();
		for ( int j = 0; j < size_dim; ++j )
		{
			str_key += ref_vec[j];
		}
		ref_comdata.map_comdata[str_key].swap(ref_vec);
	}

	ComDataIndex cd_index;
	cd_index.m_dataIndex = new_index;
	return cd_index;
}

void CompareResult::GetCompareResult(const ComDataIndex& left_index, const ComDataIndex& right_index, COMPARE_TYPE com_type, 
	const std::string& result_desc, std::vector<std::vector<std::string> >& vec2_result) throw(base::Exception)
{
	if ( !left_index.IsValid(m_vComData) )
	{
		throw base::Exception(CRERR_GET_COMPARE_RESULT, "The left index (%d) is invalid in compare-data vector ! [FILE:%s, LINE:%d]", left_index.m_dataIndex, __FILE__, __LINE__);
	}

	if ( !right_index.IsValid(m_vComData) )
	{
		throw base::Exception(CRERR_GET_COMPARE_RESULT, "The right index (%d) is invalid in compare-data vector ! [FILE:%s, LINE:%d]", right_index.m_dataIndex, __FILE__, __LINE__);
	}

	if ( left_index == right_index )
	{
		throw base::Exception(CRERR_GET_COMPARE_RESULT, "The right index (%d) is a duplicate of the left index (%d) ! [FILE:%s, LINE:%d]", right_index.m_dataIndex, left_index.m_dataIndex, __FILE__, __LINE__);
	}

	CompareData* p_left_comdata  = left_index.At(m_vComData);
	CompareData* p_right_comdata = right_index.At(m_vComData);

	switch ( com_type )
	{
	case CTYPE_EQUAL:
		throw base::Exception(CRERR_GET_COMPARE_RESULT, "NOT support equal compare type ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		break;
	case CTYPE_DIFF:
		throw base::Exception(CRERR_GET_COMPARE_RESULT, "NOT support differ compare type ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		break;
	case CTYPE_LEFT:
		LeftNotInRight(p_left_comdata, p_right_comdata, false, result_desc, vec2_result);
		break;
	case CTYPE_RIGHT:
		LeftNotInRight(p_right_comdata, p_left_comdata, true, result_desc, vec2_result);
		break;
	default:
		throw base::Exception(CRERR_GET_COMPARE_RESULT, "Unknown compare type: %d ! [FILE:%s, LINE:%d]", com_type, __FILE__, __LINE__);
	}
}

void CompareResult::LeftNotInRight(CompareData* pLeft, CompareData* pRight, bool left_zero, const std::string& result_desc, std::vector<std::vector<std::string> >& vec2_result)
{
	std::vector<std::vector<std::string> > v2_res;

	CompareData::MAP_DATA::iterator it = pLeft->map_comdata.begin();
	const int VEC_SIZE = it->second.size();					// 总列数
	const int VAL_SIZE = VEC_SIZE - pLeft->dim_size;		// 值的列数

	// 零值插入的起始位置
	const int ZERO_START_POS = (left_zero ? pLeft->dim_size : VEC_SIZE);

	// 遍历 "左" 的数据
	for ( ; it != pLeft->map_comdata.end(); ++it )
	{
		// 找寻不存在于 "右" 的数据
		if ( pRight->map_comdata.find(it->first) == pRight->map_comdata.end() )
		{
			std::vector<std::string>& ref_vec = it->second;

			for ( int i = 0; i < VAL_SIZE; ++i )
			{
				ref_vec.push_back(ref_vec[pLeft->dim_size+i]);
			}

			for ( int j = 0; j < VAL_SIZE; ++j )
			{
				ref_vec.insert((ref_vec.begin()+ZERO_START_POS+j), "0");
			}

			// 最后加上结果描述
			ref_vec.push_back(result_desc);
			base::PubStr::VVectorSwapPushBack(v2_res, ref_vec);
		}
	}

	v2_res.swap(vec2_result);
}

