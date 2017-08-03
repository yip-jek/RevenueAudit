#pragma once

#include <string>
#include <vector>

// 报表输入接口
class ReportInput
{
public:
	ReportInput() {}
	virtual ~ReportInput() {}

public:
	// 获取数据组数目
	virtual size_t GetDataGroupSize() const = 0;

	// 获取数据大小
	virtual size_t GetDataSize(size_t group_index) const = 0;

	// 获取特征值（key值）
	virtual std::string GetKey(size_t group_index, size_t data_index) const = 0;

	// 导出数据
	virtual void ExportData(size_t group_index, size_t data_index, std::vector<std::string>& vec_dat) const = 0;
};


////////////////////////////////////////////////////////////////////////////////
// 一点稽核-报表输入
class YDReportInput : public ReportInput
{
public:
	YDReportInput(std::vector<std::vector<std::vector<std::string> > >& v3SrcData);
	virtual ~YDReportInput();

public:
	// 释放源数据
	void ReleaseSourceData();

	// 获取数据组数目
	virtual size_t GetDataGroupSize() const;

	// 获取数据大小
	virtual size_t GetDataSize(size_t group_index) const;

	// 获取特征值（key值）
	virtual std::string GetKey(size_t group_index, size_t data_index) const;

	// 导出数据
	virtual void ExportData(size_t group_index, size_t data_index, std::vector<std::string>& vec_dat) const;

private:
	// 索引是否有效
	bool IsIndexValid(size_t group_index, size_t data_index) const;

private:
	std::vector<std::vector<std::vector<std::string> > >* m_pV3SrcData;			// Hive源数据集
};

