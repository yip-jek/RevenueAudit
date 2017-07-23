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
		std::string key;

		m_mReportStatData.clear();
		std::map<std::string, std::vector<std::string> >::iterator it = m_mReportStatData.end();

		const int GROUP_SIZE = pInput->GetDataGroupSize();
		for ( int i = 0; i < GROUP_SIZE; ++i )
		{
			const int DATA_SIZE = pInput->GetDataSize(i);
			for ( int j = 0; j < DATA_SIZE; ++j )
			{
				key = pInput->GetKey(i, j);

			}
		}

		ExportReportData(pOutput);
	}

	////////////////////////////////////////////////////////////////////////////////////////
	std::string str_tmp;
	std::string m_key;

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

		const size_t VEC2_SIZE = ref_vec2.size();
		for ( size_t j = 0; j < VEC2_SIZE; ++j )
		{
			std::vector<std::string>& ref_vec1 = ref_vec2[j];

			// 组织key值
			m_key.clear();
			const int VEC1_SIZE = ref_vec1.size();
			const int DIM_SIZE = VEC1_SIZE - 1;
			for ( int k = 0; k < DIM_SIZE; ++k )
			{
				std::string& ref_str = ref_vec1[k];
				m_key += ref_str;
			}

			// 值所在的列序号
			const int VAL_INDEX = DIM_SIZE + i;

			it = mReportStatData.find(m_key);
			if ( it != mReportStatData.end() )		// key值存在
			{
				std::vector<std::string>& ref_m_v = it->second;

				ref_m_v[VAL_INDEX] = ref_vec1[DIM_SIZE];
			}
			else		// key值不存在
			{
				str_tmp = ref_vec1[DIM_SIZE];
				ref_vec1[DIM_SIZE] = "NULL";
				//ref_vec1[DIM_SIZE] = "";		// 置为空

				const int TOTAL_SIZE = VEC3_SIZE + VEC1_SIZE - 1;
				for ( int l = VEC1_SIZE; l < TOTAL_SIZE; ++l )
				{
					ref_vec1.push_back("NULL");
					//ref_vec1.push_back("");		// 置为空
				}
				ref_vec1[VAL_INDEX] = str_tmp;

				mReportStatData[m_key].swap(ref_vec1);
			}
		}
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

