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
public:
	YCStatFactor();
	virtual ~YCStatFactor();

	static const char* const S_TOP_PRIORITY;			// 最高优先级 (差异汇总)
	static const int S_BASE_COLUMN_SIZE     = 2;		// 基础数据列数
	static const int S_CATEGORY_COLUMN_SIZE = 3;		// 分列数据列数

public:
	// 获取统计指标ID
	std::string GetStatID() const;

	// 获取关联报表
	std::string GetStatReport() const;

	// 载入规则因子信息
	void LoadStatInfo(std::vector<YCStatInfo>& vec_statinfo) throw(base::Exception);

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
	void ExpandCategoryStatInfo(const YCStatInfo& st_info, bool agg, std::vector<YCCategoryFactor>& vec_ctgfctr) throw(base::Exception);

	// 匹配一般分类因子
	void MatchCategoryFactor(const std::string& dim, const std::string& dim_a, const std::string& dim_b, std::vector<YCCategoryFactor>& vec_ctgfctr);

private:
	base::Log*                              m_pLog;
	std::string                             m_statID;					// 统计指标ID
	std::string                             m_statReport;				// 关联报表

private:
	std::map<std::string, YCFactor>         m_mFactor;					// 因子对
	std::map<int, std::vector<YCStatInfo> > m_mvStatInfo;				// 规则因子信息列表
	std::vector<YCStatInfo>                 m_vTopStatInfo;				// （最高优先级）规则因子信息列表

private:
	std::map<std::string, std::vector<YCCategoryFactor> > m_mvCategoryFactor;		// 分类因子对
};

