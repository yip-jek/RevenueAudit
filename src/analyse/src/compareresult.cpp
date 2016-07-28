#include "compareresult.h"
#include "pubstr.h"

CompareResult::CompareResult()
{
}

CompareResult::~CompareResult()
{
}

ComDataIndex CompareResult::SetCompareData(std::vector<std::vector<std::string> >& vec2_data, int size_dim, int size_val)
{
	if ( vec2_data.empty() )
	{
		throw base::Exception(CRERR_SET_COMPARE_DATA, "NO compare data to be set! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( size_dim <= 0 )
	{
		throw base::Exception(CRERR_SET_COMPARE_DATA, "Invalid dim size: %d [FILE:%s, LINE:%d]", size_dim, __FILE__, __LINE__);
	}

	if ( size_val <= 0 )
	{
		throw base::Exception(CRERR_SET_COMPARE_DATA, "Invalid val size: %d [FILE:%s, LINE:%d]", size_val, __FILE__, __LINE__);
	}

	m_vComData.push_back(CompareData());
	int new_index = m_vComData.size() - 1;

	CompareData& ref_comdata = m_vComData[new_index];
	ref_comdata.dim_size = size_dim;
	ref_comdata.val_size = size_val;

	std::string str_key;
	const int TOTAL_SIZE = size_dim + size_val;
	const size_t VEC2_SIZE = vec2_data.size();
	for ( size_t i = 0; i < VEC2_SIZE; ++i )
	{
		std::vector<std::string>& ref_vec = vec2_data[i];

		const int REF_VSIZE = ref_vec.size();
		if ( REF_VSIZE < TOTAL_SIZE )
		{
			throw base::Exception(CRERR_SET_COMPARE_DATA, "[Index:%llu] Compare data size [%d] less than the total size [%d]! (The total size is the sum of dim_size [%d] and val_size [%d]) [FILE:%s, LINE:%d]", (i+1), REF_VSIZE, TOTAL_SIZE, size_dim, size_val, __FILE__, __LINE__);
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

	CompareData::MAP_DATA::iterator it_left = pLeft->map_comdata.begin();
	const int TOTAL_COMPARE_SIZE = pLeft->dim_size + pLeft->val_size;						// 所有对比的列数
	const int ZERO_START_POS = (left_zero ? pLeft->dim_size : TOTAL_COMPARE_SIZE);			// 零值插入的起始位置
	const int RD_INSERT_POS = TOTAL_COMPARE_SIZE + (2 * pLeft->val_size);					// 结果描述插入的位置

	CompareData::MAP_DATA::iterator it_right = pRight->map_comdata.begin();
	// 单独显示的维度个数
	const int SINGLE_DIM_SIZE = it_right->second.size() - TOTAL_COMPARE_SIZE;		
	// 单独显示的维度插入的起始位置
	const int SINGLE_DIM_START_POS = (left_zero ? (RD_INSERT_POS+1) : (it_left->second.size()+1+(2*pLeft->val_size)));	

	// 遍历 "左" 的数据
	std::vector<std::string> vec_str;
	for ( ; it_left != pLeft->map_comdata.end(); ++it_left )
	{
		// 找寻不存在于 "右" 的数据
		it_right = pRight->map_comdata.find(it_left->first);
		if ( it_right == pRight->map_comdata.end() )
		{
			vec_str = it_left->second;

			for ( int i = 0; i < pLeft->val_size; ++i )
			{
				vec_str.insert((vec_str.begin()+TOTAL_COMPARE_SIZE+i), vec_str[pLeft->dim_size+i]);
			}

			for ( int j = 0; j < pLeft->val_size; ++j )
			{
				vec_str.insert((vec_str.begin()+ZERO_START_POS+j), "0");
			}

			// 再加上结果描述
			vec_str.insert((vec_str.begin()+RD_INSERT_POS), result_desc);

			// 加上另一侧的单独显示的列
			// 但由于另一侧不存在，所以填空 (NULL)
			for ( int k = 0; k < SINGLE_DIM_SIZE; ++k )
			{
				vec_str.insert((vec_str.begin()+SINGLE_DIM_START_POS+k), "NULL");
			}

			base::PubStr::VVectorSwapPushBack(v2_res, vec_str);
		}
	}

	v2_res.swap(vec2_result);
}

