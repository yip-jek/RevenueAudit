#pragma once

#include <map>
#include "ycinfo.h"

namespace base
{
class Log;
}

// （业财）统计因子类
class YCStatFactor
{
public:
	typedef std::vector<std::string>		VEC_STRING;
	typedef std::vector<VEC_STRING>			VEC2_STRING;
	typedef std::vector<VEC2_STRING>		VEC3_STRING;

	typedef std::vector<YCStatInfo>			VEC_STATINFO;
	typedef std::map<int, VEC_STATINFO>		MAP_VEC_STATINFO;

	static const char* const S_TOP_PRIORITY;			// 最高优先级 (差异汇总)

public:
	YCStatFactor(const std::string& etl_day, YCTaskReq& task_req);
	virtual ~YCStatFactor();

public:
	// 获取统计指标ID
	virtual std::string GetStatID() const;

	// 获取关联报表
	virtual std::string GetStatReport() const;

	// 载入规则因子信息
	virtual void LoadStatInfo(VEC_STATINFO& vec_statinfo) throw(base::Exception);

	// 载入因子对
	virtual int LoadFactors(const VEC3_STRING& v3_data) throw(base::Exception);

	// 生成稽核统计结果
	virtual void MakeResult(VEC3_STRING& v3_result) throw(base::Exception) = 0;

protected:
	// 释放因子资源
	virtual void ReleaseFactors() = 0;

	// 载入单个因子对
	virtual void LoadOneFactor(const std::string& dim, const VEC_STRING& vec_dat) throw(base::Exception) = 0;

	// 生成统计结果
	virtual void GenerateStatResult(VEC2_STRING& v2_result) throw(base::Exception);

	// 由因子规则生成结果数据
	virtual void MakeStatInfoResult(int batch, const YCStatInfo& st_info, bool agg, VEC2_STRING& vec2_result) throw(base::Exception) = 0;

	// 计算单个因子
	virtual double OperateOneFactor(double left, const std::string& op, const std::string& factor) throw(base::Exception);

	// 结果数据类型转换：Double -> String，保留2位小数，“四舍五入”
	std::string ConvertResultType(double result);

protected:
	base::Log*       m_pLog;
	std::string      m_statID;					// 统计指标ID
	std::string      m_statReport;				// 关联报表
	std::string      m_etlDay;					// 采集账期
	YCTaskReq&       m_refTaskReq;				// 任务请求信息
	MAP_VEC_STATINFO m_mvStatInfo;				// 规则因子信息列表
};

