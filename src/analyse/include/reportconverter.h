#pragma once

#include "exception.h"

class ReportInput;
class ReportOutput;

// 报表转换
class ReportConverter
{
public:
	ReportConverter();
	virtual ~ReportConverter();

public:
	// 源数据转换为报表数据
	void ConvertToReportStatData(const ReportInput* pRInput, ReportOutput* pROutput);

};

