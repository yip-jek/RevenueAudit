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

	static const char* const S_TOP_PRIORITY;			// 最高优先级

public:
	// 获取统计指标ID
	std::string GetStatID() const;

	// 获取关联报表
	std::string GetStatReport() const;

	// 载入规则因子信息
	void LoadStatInfo(std::vector<YCStatInfo>& vec_statinfo) throw(base::Exception);

	// 载入维度因子对
	int LoadDimFactor(std::vector<std::vector<std::vector<std::string> > >& v3_data) throw(base::Exception);

	// 生成结果
	void GenerateResult(int batch, const std::string& city, std::vector<std::vector<std::string> >& v2_result) throw(base::Exception);

private:
	// 计算组合因子的维度值
	std::string CalcComplexFactor(const std::string& cmplx_fctr_fmt) throw(base::Exception);

	// 计算单个因子
	void OperateOneFactor(std::string& result, const std::string& op, const std::string& factor) throw(base::Exception);

private:
	base::Log*                              m_pLog;
	std::string                             m_statID;				// 统计指标ID
	std::string                             m_statReport;			// 关联报表

private:
	std::map<std::string, std::string>      m_mDimFactor;			// 维度因子对
	std::map<int, std::vector<YCStatInfo> > m_mvStatInfo;			// 规则因子信息列表
	std::vector<YCStatInfo>                 m_vTopStatInfo;			// （最高优先级）规则因子信息列表
};

