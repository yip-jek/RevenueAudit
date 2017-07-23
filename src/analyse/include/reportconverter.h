#pragma once

#include <string>
#include <map>
#include <vector>

class ReportInput;
class ReportOutput;

// 报表转换
class ReportConverter
{
public:
	ReportConverter();
	virtual ~ReportConverter();

public:
	// 报表数据转换
	void ReportStatDataConvertion(const ReportInput* pInput, ReportOutput* pOutput);

private:
	// 数据合并
	void MergeData(const std::string& key, const std::vector<std::string>& vec_data, int val_size, int index);

	// 导出报表数据
	void ExportReportData(ReportOutput* pOutput);

private:
	std::map<std::string, std::vector<std::string> > m_mReportStatData;			// 报表中间数据
};

