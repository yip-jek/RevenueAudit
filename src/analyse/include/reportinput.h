#pragma once

#include <vector>
#include "exception.h"

// 报表输入接口
class ReportInput
{
public:
	ReportInput() {}
	virtual ~ReportInput() {}

public:
	// 获取数据组数目
	virtual unsigned int GetDataGroupSize() const = 0;

	// 获取数据大小
	virtual unsigned int GetDataSize(unsigned int group_index) const throw(base::Exception) = 0;

	// 获取特征值（key值）
	virtual std::string GetKey(unsigned int group_index, unsigned int date_index) const throw(base::Exception) = 0;

	// 导出数据
	virtual void ExportData(unsigned int group_index, unsigned int date_index, std::vector<std::string>& vec_dat) const throw(base::Exception) = 0;
};

// 一点稽核-报表输入
class YDReportInput : public ReportInput
{
public:
	YDReportInput(std::vector<std::vector<std::vector<std::string> > >* pV3SrcData);
	virtual ~YDReportInput();

public:
	// 获取数据组数目
	virtual unsigned int GetDataGroupSize() const;

	// 获取数据大小
	virtual unsigned int GetDataSize(unsigned int group_index) const throw(base::Exception);

	// 获取特征值（key值）
	virtual std::string GetKey(unsigned int group_index, unsigned int date_index) const throw(base::Exception);

	// 导出数据
	virtual void ExportData(unsigned int group_index, unsigned int date_index, std::vector<std::string>& vec_dat) const throw(base::Exception);

	// 释放源数据
	void ReleaseSourceData();

private:
	std::vector<std::vector<std::vector<std::string> > >* m_pV3SrcData;			// Hive源数据集
};

