#pragma once

#include <vector>
#include "exception.h"

// (业财稽核) 详情表因子
class YCFactor_XQB
{
public:
	typedef std::vector<std::string>		VEC_STRING;

public:
	YCFactor_XQB(int item_size);
	virtual ~YCFactor_XQB();

public:
	// 导入数据
	bool Import(const VEC_STRING& vec_dat);

	// 导出数据
	void Export(VEC_STRING& vec_dat) const;

	// 输出日志信息
	std::string LogPrintInfo() const;

	// 总个数
	virtual int GetAllSize() const;

	// 区域和项目个数
	int GetAreaItemSize() const;

	// VALUE 个数
	virtual int GetValueSize() const = 0;

	// 获取维度ID
	std::string GetDimID() const;

	// 获取区域
	std::string GetArea() const;

	// 设置维度ID
	void SetDimID(const std::string& dim);

	// 设置区域
	void SetArea(const std::string& area);

	// 导入项目内容
	bool ImportItems(const VEC_STRING& vec_item);

	// 导出项目内容
	void ExportItems(VEC_STRING& vec_item);

	// 导入 VALUE
	virtual bool ImportValue(const VEC_STRING& vec_value) = 0;

	// 导出 VALUE
	virtual void ExportValue(VEC_STRING& vec_value) const = 0;

protected:
	// 初始化项目内容
	void InitItems() throw(base::Exception);

	// 追加 VALUE 的日志信息
	virtual void AddValueLogInfo(std::string& info) const = 0;

protected:
	std::string m_dimID;				// 维度ID
	std::string m_area;					// 区域

protected:
	const int   ITEM_SIZE;				// 项目数
	VEC_STRING  m_vecItems;				// 项目内容（一个或多个）
};


////////////////////////////////////////////////////////////////////////////////
// (业财稽核) 详情表（业务侧、财务侧）因子
class YCFactor_XQB_YCW : public YCFactor_XQB
{
public:
	YCFactor_XQB_YCW(int item_size);
	virtual ~YCFactor_XQB_YCW();

	static const int S_VALUE_SIZE = 1;			// 只有1个VALUE

public:
	// 总个数
	virtual int GetAllSize() const;

	// VALUE 个数
	virtual int GetValueSize() const;

	// 导入 VALUE
	virtual bool ImportValue(const VEC_STRING& vec_value);

	// 导出 VALUE
	virtual void ExportValue(VEC_STRING& vec_value) const;

protected:
	// 追加 VALUE 的日志信息
	virtual void AddValueLogInfo(std::string& info) const;

protected:
	std::string m_value;				// 业务账 或 财务账
};


////////////////////////////////////////////////////////////////////////////////
// (业财稽核) 详情表（省）因子
class YCFactor_XQB_GD : public YCFactor_XQB
{
public:
	YCFactor_XQB_GD(int item_size);
	virtual ~YCFactor_XQB_GD();

	static const int S_VALUE_SIZE = 2;			// 有2个VALUE

public:
	// 总个数
	virtual int GetAllSize() const;

	// VALUE 个数
	virtual int GetValueSize() const;

	// 导入 VALUE
	virtual bool ImportValue(const VEC_STRING& vec_value);

	// 导出 VALUE
	virtual void ExportValue(VEC_STRING& vec_value) const;

protected:
	// 追加 VALUE 的日志信息
	virtual void AddValueLogInfo(std::string& info) const;

protected:
	std::string  m_valueYW;				// 业务账
	std::string  m_valueCW;				// 财务账
};

