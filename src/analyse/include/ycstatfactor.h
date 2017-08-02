#pragma once

#include <map>
#include "ycinfo.h"

namespace base
{
class Log;
}

// （业财）规则因子类
class YCStatFactor
{
	typedef std::vector<YCStatInfo>						VEC_STATINFO;
	typedef std::vector<YCCategoryFactor>				VEC_CATEGORYFACTOR;

	typedef std::map<std::string, YCFactor>				MAP_FACTOR;
	typedef std::map<std::string, YCPairCategoryFactor> MAP_PAIRCATEGORYFACTOR;
	typedef std::map<int, VEC_STATINFO>					MAP_VEC_STATINFO;
	typedef std::map<std::string, VEC_CATEGORYFACTOR>	MAP_VEC_CATEGORYFACTOR;

public:
	YCStatFactor();
	virtual ~YCStatFactor();

	static const char* const S_TOP_PRIORITY;			// 最高优先级 (差异汇总)
	static const int S_CATEGORY_COLUMN_SIZE = 3;		// 分列数据列数

public:
	// 获取统计指标ID
	std::string GetStatID() const;

	// 获取关联报表
	std::string GetStatReport() const;

	// 载入规则因子信息
	void LoadStatInfo(VEC_STATINFO& vec_statinfo) throw(base::Exception);

	// 载入因子对
	int LoadFactor(std::vector<std::vector<std::vector<std::string> > >& v3_data) throw(base::Exception);

	// 生成报表结果
	void GenerateResult(int batch, const std::string& city, std::vector<std::vector<std::string> >& v2_result) throw(base::Exception);

	// 生成差异汇总结果
	void GenerateDiffSummaryResult(const std::string& city, std::vector<std::vector<std::string> >& v2_result) throw(base::Exception);

private:
	// 计算组合因子的维度值
	std::string CalcComplexFactor(const std::string& cmplx_fctr_fmt) throw(base::Exception);

	// 计算组合分类因子的维度值
	std::string CalcCategoryFactor(const std::string& ctg_fmt) throw(base::Exception);

	// 生成分类因子维度ID
	std::string ExtendCategoryDim(const std::string& dim, int index);

	// 获取指定组合分类因子的维度值
	bool GetCategoryFactorValue(const std::string& ctg_fmt, int index, std::string& val);

	// 计算单个因子
	void OperateOneFactor(std::string& result, const std::string& op, const std::string& factor) throw(base::Exception);

	// 是否存在于分类因子列表中
	bool IsCategoryDim(const std::string& dim);

	// 由因子规则生成结果数据
	void MakeStatInfoResult(int batch, const std::string& city, const YCStatInfo& st_info, bool agg, std::vector<std::vector<std::string> >& vec2_result) throw(base::Exception);

	// 扩展分类因子信息
	void ExpandCategoryStatInfo(const YCStatInfo& st_info, bool agg, VEC_CATEGORYFACTOR& vec_ctgfctr) throw(base::Exception);

	// 匹配一般分类因子
	void MatchCategoryFactor(const std::string& dim, const std::string& dim_a, const std::string& dim_b, VEC_CATEGORYFACTOR& vec_ctgfctr);

private:
	base::Log*             m_pLog;
	std::string            m_statID;					// 统计指标ID
	std::string            m_statReport;				// 关联报表

private:
	MAP_FACTOR             m_mFactor;					// 因子对
	MAP_VEC_STATINFO       m_mvStatInfo;				// 规则因子信息列表
	VEC_STATINFO           m_vTopStatInfo;				// （最高优先级）规则因子信息列表
	MAP_VEC_CATEGORYFACTOR m_mvCategoryFactor;			// 分类因子对
};

