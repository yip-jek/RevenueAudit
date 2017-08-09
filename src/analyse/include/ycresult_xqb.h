#pragma once

#include <vector>
#include "exception.h"

class YCFactor_XQB;

// (业财稽核) 详情表因子结果信息
class YCResult_XQB
{
public:
	typedef std::vector<std::string>		VEC_STRING;

	static const int S_PUBLIC_MEMBERS = 4;			// 公开成员数

	// 结果因子类型
	enum RESULT_FACTOR_TYPE
	{
		RFT_XQB_YCW = 1;			// 详情表（业务侧、财务侧）
		RFT_XQB_GD  = 2;			// 详情表（省）
	};

public:
	YCResult_XQB(RESULT_FACTOR_TYPE rf_type);
	virtual ~YCResult_XQB();

public:
	// 从因子导入
	bool ImportFromFactor(const YCFactor_XQB* p_factor);

	// 导入数据
	bool Import(const VEC_STRING& vec_dat);

	// 导出数据
	void Export(VEC_STRING& vec_dat) const;

	// 输出日志信息
	std::string LogPrintInfo() const;

private:
	// 创建详情因子
	void CreateFactor() throw(base::Exception);

	// 释放详情因子
	void ReleaseFactor();

public:
	std::string bill_cyc;				// 账期
	std::string city;					// 地市
	std::string type;					// 类型：0-固定项，1-浮动项
	int         batch;					// 批次

private:
	RESULT_FACTOR_TYPE m_rfType;			// 因子类型
	YCFactor_XQB*      m_pFactor;			// 详情表因子
};

