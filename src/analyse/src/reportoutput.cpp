#include "reportoutput.h"

YDReportOutput::YDReportOutput(std::vector<std::vector<std::string> >* pV2ReportData)
:m_pV2ReportData(pV2ReportData)
{
}

YDReportOutput::~YDReportOutput()
{
}

void YDReportOutput::ImportData(std::vector<std::vector<std::string> >& vec2_dat)
{
	if ( m_pV2ReportData != NULL )
	{
		m_pV2ReportData->swap(vec2_dat);
	}
}

