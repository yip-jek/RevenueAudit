#pragma once

#include <vector>
#include "exception.h"

// (业财稽核) 详情表因子
class YCFactor_XQB
{
public:
	typedef std::vector<std::string>		VEC_STRING;

public:
	YCFactor_XQB(int item_size, int val_size);
	virtual ~YCFactor_XQB();

public:
	// 导入数据
	bool Import(const VEC_STRING& vec_dat);

	// 导出数据
	void Export(VEC_STRING& vec_dat) const;

	// 输出日志信息
	std::string LogPrintInfo() const;

	// 总个数
	int GetAllSize() const;

	// 区域和项目个数
	int GetAreaItemSize() const;

	// VALUE 个数
	int GetValueSize() const;

	// 获取维度ID
	std::string GetDimID() const;

	// 获取区域
	std::string GetArea() const;

	// 获取维度
	std::string GetItem(size_t index) const;

	// 获取 VALUE
	std::string GetValue(size_t index) const;

	// 设置维度ID
	void SetDimID(const std::string& dim);

	// 设置区域
	void SetArea(const std::string& area);

	// 导入项目内容
	bool ImportItems(const VEC_STRING& vec_item);

	// 导出项目内容
	void ExportItems(VEC_STRING& vec_item) const;

	// 导入 VALUE
	bool ImportValue(const VEC_STRING& vec_value);

	// 导出 VALUE
	void ExportValue(VEC_STRING& vec_value) const;

private:
	// 初始化
	void Init(int item_size, int val_size);

private:
	std::string m_dimID;				// 维度ID
	std::string m_area;					// 区域

private:
	VEC_STRING  m_vecItems;				// 项目内容 (1..N)
	VEC_STRING  m_vecVals;				// 值：业务账、财务账等
};

