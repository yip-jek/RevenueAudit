#pragma once

#include <vector>
#include "exception.h"

// 报表输出接口
class ReportOutput
{
public:
	ReportOutput() {}
	virtual ~ReportOutput() {}

public:
	// 导入数据
	virtual void ImportData(std::vector<std::vector<std::string> >& vec2_dat) = 0;
};


////////////////////////////////////////////////////////////////////////////////
// 一点稽核-报表输出
class YDReportOutput : public ReportOutput
{
public:
	YDReportOutput(std::vector<std::vector<std::string> >* pV2ReportData);
	virtual ~YDReportOutput();

public:
	// 导入数据
	virtual void ImportData(std::vector<std::vector<std::string> >& vec2_dat);

private:
	std::vector<std::vector<std::string> >* m_pV2ReportData;			// 报表数据集
};

