#pragma once

#include <map>
#include "ycinfo.h"
#include "ycstatarithmet.h"

namespace base
{
class Log;
}

// （业财）统计因子类
class YCStatFactor
{
public:
	typedef std::vector<double>                         VEC_DOUBLE;
	typedef std::vector<std::string>                    VEC_STRING;
	typedef std::vector<VEC_STRING>                     VEC2_STRING;
	typedef std::vector<VEC2_STRING>                    VEC3_STRING;

	typedef std::vector<YCStatInfo>                     VEC_STATINFO;
	typedef std::map<int, VEC_STATINFO>                 MAP_VEC_STATINFO;

	static const char* const S_TOP_PRIORITY;			// 最高优先级 (差异汇总)
	static const char* const S_ARITHMETIC;				// 四则运算标识

public:
	YCStatFactor(const std::string& etl_day, YCTaskReq& task_req);
	virtual ~YCStatFactor();

public:
	// 获取统计指标ID
	virtual std::string GetStatID() const;

	// 获取关联报表
	virtual std::string GetStatReport() const;

	// 载入规则因子信息
	virtual void LoadStatInfo(VEC_STATINFO& vec_statinfo);

	// 载入因子对
	virtual int LoadFactors(const VEC3_STRING& v3_data);

	// 生成稽核统计结果
	virtual void MakeResult(VEC3_STRING& v3_result) = 0;

	// 获取维度因子值
	virtual void GetDimFactorValue(const std::string& dim, VEC_STRING& vec_val) = 0;

protected:
	// 释放因子资源
	virtual void ReleaseFactors() = 0;

	// 载入单个因子对
	virtual void LoadOneFactor(const std::string& dim, const VEC_STRING& vec_dat) = 0;

	// 生成统计结果
	virtual void GenerateStatResult(VEC2_STRING& v2_result);

	// 由因子规则生成结果数据
	virtual void MakeStatInfoResult(int batch, const YCStatInfo& st_info, bool agg, VEC2_STRING& vec2_result) = 0;

	// 计算单个因子
	virtual double OperateOneFactor(double left, const std::string& op, const std::string& factor);

	// 结果数据类型转换：Double -> String，保留2位小数，“四舍五入”
	std::string ConvertResultType(double result);

	// 是否为四则运算表达式？
	// 四则运算表达式格式：以"[ARITHMETIC]"为表达式首部
	bool IsArithmetic(const std::string& expr, std::string& arith_exp);

protected:
	base::Log*       m_pLog;
	std::string      m_statID;					// 统计指标ID
	std::string      m_statReport;				// 关联报表
	std::string      m_etlDay;					// 采集账期
	YCTaskReq&       m_refTaskReq;				// 任务请求信息
	YCStatArithmet   m_statArithmet;			// 四则运算统计
	MAP_VEC_STATINFO m_mvStatInfo;				// 规则因子信息列表
};

