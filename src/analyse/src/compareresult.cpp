#include "compareresult.h"

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
		throw base::Exception(CRERR_SET_COMPARE_DATA,
	}

	if ( size_dim <= 0 )
	{
		throw base::Exception(CRERR_SET_COMPARE_DATA,
	}

	m_vComData.push_back(CompareData());
	int new_index = m_vComData.size() - 1;

	CompareData& ref_comdata = m_vComData[new_index];
	ref_comdata.dim_size     = size_dim;

	const size_t VEC2_SIZE = vec2_data.size();
	for ( size_t i = 0; i < VEC2_SIZE; ++i )
	{
		std::vector<std::string>& ref_vec = vec2_data[i];

		const int REF_VSIZE = ref_vec.size();
		if ( size_dim >= REF_VSIZE )
		{
			throw base::Exception(CRERR_SET_COMPARE_DATA,
		}

	}

	ComDataIndex cd_index;
	cd_index.m_dataIndex = new_index;
	return cd_index;
}

void CompareResult::GetCompareResult(const ComDataIndex& left_index, const ComDataIndex& right_index, COMPARE_TYPE com_type, 
	std::string& result_desc, std::vector<std::vector<std::string> >& vec2_result) throw(base::Exception)
{
	if ( !left_index.IsValid(m_vComData) )
	{
		throw base::Exception(CRERR_GET_COMPARE_RESULT,
	}

	if ( !right_index.IsValid(m_vComData) )
	{
		throw base::Exception(CRERR_GET_COMPARE_RESULT,
	}

	if ( left_index == right_index )
	{
		throw base::Exception(CRERR_GET_COMPARE_RESULT,
	}

	CompareData* p_left_comdata  = left_index.At(m_vComData);
	CompareData* p_right_comdata = right_index.At(m_vComData);

	switch ( com_type )
	{
	case CTYPE_EQUAL:
		throw base::Exception(CRERR_GET_COMPARE_RESULT,
		break;
	case CTYPE_DIFF:
		throw base::Exception(CRERR_GET_COMPARE_RESULT,
		break;
	case CTYPE_LEFT:
		break;
	case CTYPE_RIGHT:
		break;
	default:
		throw base::Exception(CRERR_GET_COMPARE_RESULT,
	}
}

