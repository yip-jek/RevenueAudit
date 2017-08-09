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
	virtual bool Import(const VEC_STRING& vec_dat);

	// 导出数据
	virtual void Export(VEC_STRING& vec_dat) const;

	// 输出日志信息
	virtual std::string LogPrintInfo() const;

	// 总大小
	virtual int GetAllSize() const;

	// 区域和项目大小
	virtual int GetAreaItemSize() const;

	// 设置 AREA
	virtual void SetArea(const std::string& area);

	// 导入项目内容
	virtual bool ImportItems(const VEC_STRING& vec_item);

protected:
	// 初始化项目内容
	virtual void InitItems() throw(base::Exception);

	// 导入值
	virtual void ImportValue(const VEC_STRING& vec_value) = 0;

	// 导出值
	virtual void ExportValue(VEC_STRING& vec_value) const = 0;

	// 追加值的日志信息
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
	// 总大小
	virtual int GetAllSize() const;

protected:
	// 导入值
	virtual void ImportValue(const VEC_STRING& vec_value);

	// 导出值
	virtual void ExportValue(VEC_STRING& vec_value) const;

	// 追加值的日志信息
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
	// 导入数据
	virtual int GetAllSize() const;

protected:
	// 导入值
	virtual void ImportValue(const VEC_STRING& vec_value);

	// 导出值
	virtual void ExportValue(VEC_STRING& vec_value) const;

	// 追加值的日志信息
	virtual void AddValueLogInfo(std::string& info) const;

protected:
	std::string  m_valueYW;				// 业务账
	std::string  m_valueCW;				// 财务账
};

