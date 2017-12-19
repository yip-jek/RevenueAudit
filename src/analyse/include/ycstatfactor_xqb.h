#pragma once

#include "ycstatfactor.h"

class YCFactor_XQB;

// （业财）详情表统计因子类
class YCStatFactor_XQB : public YCStatFactor
{
public:
	typedef std::vector<double>                         VEC_DOUBLE;

	typedef std::pair<std::string, YCFactor_XQB>		PAIR_FACTOR;
	typedef std::map<std::string, YCFactor_XQB>			MAP_FACTOR;

public:
	YCStatFactor_XQB(const std::string& etl_day, YCTaskReq& task_req, int ana_type, int item_size, int val_size);
	virtual ~YCStatFactor_XQB();

public:
	// 生成稽核统计结果
	virtual void MakeResult(VEC3_STRING& v3_result) throw(base::Exception);

protected:
	// 释放因子资源
	virtual void ReleaseFactors();

	// 载入单个因子对
	virtual void LoadOneFactor(const std::string& dim, const VEC_STRING& vec_dat) throw(base::Exception);

	// 由因子规则生成结果数据
	virtual void MakeStatInfoResult(int batch, const YCStatInfo& st_info, bool agg, VEC2_STRING& vec2_result) throw(base::Exception);

private:
	// 获取结果地市信息
	std::string GetResultCity();

	// 计算组合因子
	void CalcComplexFactor(const std::string& cmplx_fmt, YCFactor_XQB& factor) throw(base::Exception);

	// 计算单个因子
	void CalcOneFactor(VEC_DOUBLE& vec_result, const std::string& op, const std::string& dim) throw(base::Exception);

	// 结果数据集类型转换
	void ConvertVectorResultType(const VEC_DOUBLE& vec_dou, VEC_STRING& vec_result);

private:
	const int  ANA_TYPE;				// 分析类型
	const int  ITEM_SIZE;				// 项目数
	const int  VAL_SIZE;				// 值的数目
	MAP_FACTOR m_mFactor;				// 因子对
};

