#include "reportconverter.h"
#include "pubstr.h"
#include "reportinput.h"
#include "reportoutput.h"

ReportConverter::ReportConverter()
{
}

ReportConverter::~ReportConverter()
{
}

void ReportConverter::ReportStatDataConvertion(const ReportInput* pInput, ReportOutput* pOutput)
{
	if ( pInput != NULL && pOutput != NULL )
	{
		std::vector<std::string> vec_data;
		m_mReportStatData.clear();

		const int GROUP_SIZE = pInput->GetDataGroupSize();
		for ( int i = 0; i < GROUP_SIZE; ++i )
		{
			const int DATA_SIZE = pInput->GetDataSize(i);
			for ( int j = 0; j < DATA_SIZE; ++j )
			{
				pInput->ExportData(i, j, vec_data);
				MergeData(pInput->GetKey(i, j), vec_data, GROUP_SIZE, i);
			}
		}

		ExportReportData(pOutput);
	}
}

void ReportConverter::MergeData(const std::string& key, const std::vector<std::string>& vec_data, int val_size, int index)
{
	const int DIM_SIZE  = vec_data.size() - 1;
	const int VAL_INDEX = DIM_SIZE + index;

	std::map<std::string, std::vector<std::string> >::iterator it = m_mReportStatData.find(key);
	if ( it != m_mReportStatData.end() )	// key值存在
	{
		std::vector<std::string>& ref_vec = it->second;
		ref_vec[VAL_INDEX] = vec_data[DIM_SIZE];
	}
	else	// 不存在
	{
		// 补全所有的值列
		std::vector<std::string> vec_all(vec_data);
		vec_all.insert(vec_all.end(), val_size-1, "0");

		// 将当前值，移到正确的位置
		// 考虑到 DIM_SIZE 与 VAL_INDEX 相等的情况，因此增加临时变量
		std::string curr_val = vec_all[DIM_SIZE];
		vec_all[DIM_SIZE]    = "0";
		vec_all[VAL_INDEX]   = curr_val;

		m_mReportStatData[key].swap(vec_all);
	}
}

void ReportConverter::ExportReportData(ReportOutput* pOutput)
{
	std::vector<std::vector<std::string> > vec2_reportdata;
	std::map<std::string, std::vector<std::string> >::iterator it = m_mReportStatData.begin();
	while ( it != m_mReportStatData.end() )
	{
		base::PubStr::VVectorSwapPushBack(vec2_reportdata, (it++)->second);
	}

	pOutput->ImportData(vec2_reportdata);
}

