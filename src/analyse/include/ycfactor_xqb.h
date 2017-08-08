#pragma once

#include <string>
#include <vector>

// (业财稽核) 详情表因子
class YCFactor_XQB
{
public:
	typedef std::vector<std::string>		VEC_STRING;

	static const int S_XQB_MEMBERS = 2;			// 成员数

public:
	YCFactor_XQB() {}
	virtual ~YCFactor_XQB() {}

public:
	// 导入数据
	virtual bool Import(const VEC_STRING& vec_dat) = 0;

	// 导出数据
	virtual void Export(VEC_STRING& vec_dat) const = 0;

	// 输出日志信息
	std::string LogPrintInfo() const = 0;

protected:
	std::string dim_id;					// 维度ID
	std::string area;					// 区域
};


////////////////////////////////////////////////////////////////////////////////
// (业财稽核) 详情表（业务侧、财务侧）因子
class YCFactor_XQB_YCW : public YCFactor_XQB
{
public:
	static const int S_XQBYCW_MEMBERS = 2 + YCFactor_XQB::S_XQB_MEMBERS;		// 成员数

public:
	YCFactor_XQB_YCW();
	virtual ~YCFactor_XQB_YCW();

public:
	// 导入数据
	virtual bool Import(const VEC_STRING& vec_dat);

	// 导出数据
	virtual void Export(VEC_STRING& vec_dat) const;

	// 输出日志信息
	std::string LogPrintInfo() const;

protected:
	std::string item;					// 项目内容
	std::string value;					// 值
};


////////////////////////////////////////////////////////////////////////////////
// (业财稽核) 详情表（省）因子
class YCFactor_XQB_GD : public YCFactor_XQB
{
public:
	static const int S_ONE_ITEM = 1;
	static const int S_TWO_ITEM = 2;

public:
	YCFactor_XQB_GD();
	virtual ~YCFactor_XQB_GD();

public:
	// 导入数据
	virtual bool Import(const VEC_STRING& vec_dat);

	// 导出数据
	virtual void Export(VEC_STRING& vec_dat) const;

	// 输出日志信息
	std::string LogPrintInfo() const;

protected:
	VEC_STRING  vec_items;				// 项目内容（一个或多个）
	std::string value_YW;				// 业务账
	std::string value_CW;				// 财务账
};

