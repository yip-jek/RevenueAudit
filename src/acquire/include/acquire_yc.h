#pragma once

#include "acquire.h"

// 业财稽核-采集模块
class Acquire_YC : public Acquire
{
public:
	Acquire_YC();
	virtual ~Acquire_YC();

public:
	// 载入参数配置信息
	virtual void LoadConfig() throw(base::Exception);

	// 初始化
	virtual void Init() throw(base::Exception);

	// 任务结束（资源回收）
	virtual void End(int err_code, const std::string& err_msg = std::string()) throw(base::Exception);

protected:
	// 获取后续参数任务信息
	virtual void GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception);

	// 获取任务信息
	virtual void FetchTaskInfo() throw(base::Exception);

	// 分析统计因子规则，生成业财稽核SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	virtual void TaskInfo2Sql(std::vector<std::string>& vec_sql, bool hive) throw(base::Exception);

protected:
	int                 m_ycSeqID;					// 任务流水号
	std::string         m_tabYCTaskReq;				// （业财）任务请求表
	std::vector<YCInfo> m_vecYCInfo;				// 业财稽核因子规则信息
};

