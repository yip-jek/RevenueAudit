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
	typedef std::vector<YCStatInfo>			VEC_STATINFO;
	typedef std::map<int, VEC_STATINFO>		MAP_VEC_STATINFO;

public:
	YCStatFactor(YCTaskReq& task_req);
	virtual ~YCStatFactor();

public:
	// 获取统计指标ID
	virtual std::string GetStatID() const;

	// 获取关联报表
	virtual std::string GetStatReport() const;

	// 载入规则因子信息
	virtual void LoadStatInfo(VEC_STATINFO& vec_statinfo) throw(base::Exception);

	// 载入因子对
	virtual int LoadFactor(std::vector<std::vector<std::vector<std::string> > >& v3_data) throw(base::Exception) = 0;

	// 生成稽核统计结果
	virtual void MakeResult(std::vector<std::vector<std::vector<std::string> > >& v3_result) throw(base::Exception) = 0;

protected:
	// 计算单个因子
	virtual void OperateOneFactor(std::string& result, const std::string& op, const std::string& factor) throw(base::Exception);

protected:
	base::Log*       m_pLog;
	std::string      m_statID;					// 统计指标ID
	std::string      m_statReport;				// 关联报表
	YCTaskReq*       m_pTaskReq;				// 任务请求信息
	MAP_VEC_STATINFO m_mvStatInfo;				// 规则因子信息列表
};

