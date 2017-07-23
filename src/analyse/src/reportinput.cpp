#include "reportinput.h"

YDReportInput::YDReportInput(std::vector<std::vector<std::vector<std::string> > >* pV3SrcData)
{
}

YDReportInput::~YDReportInput()
{
}

unsigned int YDReportInput::GetDataGroupSize() const
{
}

unsigned int YDReportInput::GetDataSize(unsigned int group_index) const throw(base::Exception)
{
}

std::string YDReportInput::GetKey(unsigned int group_index, unsigned int date_index) const throw(base::Exception)
{
}

void YDReportInput::ExportData(unsigned int group_index, unsigned int date_index, std::vector<std::string>& vec_dat) const throw(base::Exception)
{
}

void YDReportInput::ReleaseSourceData()
{
}

