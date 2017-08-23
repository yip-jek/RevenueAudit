#include "reportoutput.h"

// 一点稽核-报表输出
YDReportOutput::YDReportOutput(std::vector<std::vector<std::string> >& v2ReportData)
:m_refV2ReportData(v2ReportData)
{
}

YDReportOutput::~YDReportOutput()
{
}

void YDReportOutput::ImportData(std::vector<std::vector<std::string> >& vec2_dat)
{
	m_refV2ReportData.swap(vec2_dat);
}

