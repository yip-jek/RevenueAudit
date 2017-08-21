#pragma once

#include <vector>
#include "exception.h"
#include "ycfactor_xqb.h"

// (业财稽核) 详情表因子结果信息
class YCResult_XQB
{
private:	// 禁止赋值操作
	YCResult_XQB& operator = (const YCResult_XQB& ycr);

public:
	typedef std::vector<std::string>		VEC_STRING;

	static const int S_PUBLIC_MEMBERS = 4;			// 公开成员数

public:
	YCResult_XQB(int ana_type, int item_size, int val_size);
	virtual ~YCResult_XQB();

public:
	// 导入数据
	bool Import(const VEC_STRING& vec_dat);

	// 导出数据
	void Export(VEC_STRING& vec_dat) const;

	// 输出日志信息
	std::string LogPrintInfo() const;

	// 导入因子
	void ImportFactor(const YCFactor_XQB& factor);

	/// 获取因子里的内容
	std::string GetFactorDim() const;				// 获取因子维度
	std::string GetFactorArea() const;				// 获取因子区域
	std::string GetFactorFirstItem() const;			// 获取首个项目内容
	std::string GetFactorFirstValue() const;		// 获取首个VALUE

public:
	std::string bill_cyc;				// 账期
	std::string city;					// 地市
	std::string type;					// 类型：0-固定项，1-浮动项
	int         batch;					// 批次

private:
	const int    ANA_TYPE;				// 分析类型
	YCFactor_XQB m_factor;				// 详情表因子
};

