#pragma once

#include "ycstatfactor.h"

// （业财）详情表统计因子类
class YCStatFactor_XQB : public YCStatFactor
{
public:
	typedef std::map<std::string, YCFactor_XQB*>		MAP_FACTOR;

public:
	YCStatFactor_XQB(const std::string& etl_day, int ana_type, YCTaskReq& task_req);
	virtual ~YCStatFactor_XQB();

public:
	// 生成稽核统计结果
	virtual void MakeResult(VEC3_STRING& v3_result) throw(base::Exception);

protected:
	// 清空旧因子对
	virtual void ClearOldFactors();

	// 载入单个因子对
	virtual void LoadOneFactor(const std::string& dim, const VEC_STRING& vec_dat) throw(base::Exception);

	// 由因子规则生成结果数据
	virtual void MakeStatInfoResult(int batch, const YCStatInfo& st_info, bool agg, VEC2_STRING& vec2_result) throw(base::Exception);

private:
	// 计算组合因子
	void CalcComplexFactor(const std::string& cmplx_fmt, YCFactor_XQB* p_factor) throw(base::Exception);

private:
	MAP_FACTOR m_mFactor;
};

