#include "reportinput.h"

// 一点稽核-报表输入
YDReportInput::YDReportInput(std::vector<std::vector<std::vector<std::string> > >& v3SrcData)
:m_refV3SrcData(v3SrcData)
{
}

YDReportInput::~YDReportInput()
{
}

void YDReportInput::ReleaseSourceData()
{
	// Release std::vector
	std::vector<std::vector<std::vector<std::string> > >().swap(m_refV3SrcData);
}

size_t YDReportInput::GetDataGroupSize() const
{
	return m_refV3SrcData.size();
}

size_t YDReportInput::GetDataSize(size_t group_index) const
{
	if ( group_index < m_refV3SrcData.size() )
	{
		return m_refV3SrcData[group_index].size();
	}

	return 0;
}

std::string YDReportInput::GetKey(size_t group_index, size_t data_index) const
{
	if ( IsIndexValid(group_index, data_index) )
	{
		std::vector<std::string>& ref_vec = m_refV3SrcData[group_index][data_index];

		std::string key;
		const int DIM_SIZE = ref_vec.size() - 1;
		for ( int i = 0; i < DIM_SIZE; ++i )
		{
			key += ref_vec[i];
		}

		return key;
	}

	return "";
}

void YDReportInput::ExportData(size_t group_index, size_t data_index, std::vector<std::string>& vec_dat) const
{
	if ( IsIndexValid(group_index, data_index) )
	{
		vec_dat = m_refV3SrcData[group_index][data_index];
	}
}

bool YDReportInput::IsIndexValid(size_t group_index, size_t data_index) const
{
	if ( group_index < m_refV3SrcData.size() )
	{
		return (data_index < m_refV3SrcData[group_index].size());
	}

	return false;
}

