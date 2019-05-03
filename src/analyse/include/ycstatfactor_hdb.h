#pragma once

#include "ycstatfactor.h"

// （业财）核对表统计因子类
class YCStatFactor_HDB : public YCStatFactor
{
public:
	typedef std::vector<YCCategoryFactor>               VEC_CATEGORYFACTOR;
	typedef std::map<std::string, std::string>          MAP_STRING;
	typedef std::map<std::string, YCPairCategoryFactor> MAP_PAIRCATEGORYFACTOR;
	typedef std::map<std::string, VEC_CATEGORYFACTOR>   MAP_VEC_CATEGORYFACTOR;

public:
	YCStatFactor_HDB(const std::string& etl_day, YCTaskReq& task_req);
	virtual ~YCStatFactor_HDB();

public:
	// 载入规则因子信息
	virtual void LoadStatInfo(VEC_STATINFO& vec_statinfo);

	// 生成稽核统计结果
	virtual void MakeResult(VEC3_STRING& v3_result);

	// 获取维度因子值
	virtual void GetDimFactorValue(const std::string& dim, VEC_STRING& vec_val);

protected:
	// 释放因子资源
	virtual void ReleaseFactors();

	// 载入单个因子对
	virtual void LoadOneFactor(const std::string& dim, const VEC_STRING& vec_dat);

	// 由因子规则生成结果数据
	virtual void MakeStatInfoResult(int batch, const YCStatInfo& st_info, bool agg, VEC2_STRING& vec2_result);

private:
	// 生成差异汇总结果
	void GenerateDiffSummaryResult(VEC2_STRING& v2_result);

	// 计算组合因子的维度值
	std::string CalcComplexFactor(const std::string& cmplx_fmt);

	// 计算四则运算组合因子
	std::string CalcArithmeticFactor(const std::string& expr);

	// 计算单个因子的维度值
	double CalcOneFactor(double result, const std::string& op, const std::string& dim);

	// 计算组合分类因子的维度值
	std::string CalcCategoryFactor(const std::string& ctg_fmt);

	// 生成分类因子维度ID
	std::string ExtendCategoryDim(const std::string& dim, int index);

	// 获取指定组合分类因子的维度值
	bool GetCategoryFactorValue(const std::string& ctg_fmt, int index, std::string& val);

	// 是否存在于分类因子列表中
	bool IsCategoryDim(const std::string& dim);

	// 扩展分类因子信息
	void ExpandCategoryStatInfo(const YCStatInfo& st_info, bool agg, VEC_CATEGORYFACTOR& vec_ctgfctr);

	// 匹配一般分类因子
	void MatchCategoryFactor(const std::string& dim, const std::string& dim_a, const std::string& dim_b, VEC_CATEGORYFACTOR& vec_ctgfctr);

private:
	MAP_STRING             m_mFactor;					// 因子对
	VEC_STATINFO           m_vTopStatInfo;				// （最高优先级）规则因子信息列表
	MAP_VEC_CATEGORYFACTOR m_mvCategoryFactor;			// 分类因子对
};

