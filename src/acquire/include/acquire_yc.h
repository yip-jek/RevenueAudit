#pragma once

#include "acquire.h"

// 业财稽核-采集模块
class Acquire_YC : public Acquire
{
public:
	Acquire_YC();
	virtual ~Acquire_YC();

	static const char* const S_YC_ETLRULE_TYPE;			// 业财稽核-采集规则类型

public:
	// 载入参数配置信息
	virtual void LoadConfig() throw(base::Exception);

	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();

	// 初始化
	virtual void Init() throw(base::Exception);

	// 任务结束（资源回收）
	virtual void End(int err_code, const std::string& err_msg = std::string()) throw(base::Exception);

protected:
	// 获取后续参数任务信息
	virtual void GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception);

	// 获取任务信息
	virtual void FetchTaskInfo() throw(base::Exception);

	// 检查采集任务信息
	virtual void CheckTaskInfo() throw(base::Exception);

	// 检查源表是否存在
	virtual void CheckSourceTable(bool hive) throw(base::Exception);

	// 分析统计因子规则，生成业财稽核SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	virtual void TaskInfo2Sql(std::vector<std::string>& vec_sql, bool hive) throw(base::Exception);

	// 扩展采集 SQL 语句的条件：增加账期、地市和批次
	void ExtendSQLCondition(std::string& sql) throw(base::Exception);

protected:
	std::string         m_tabYCTaskReq;				// （业财）任务请求表
	std::string         m_tabStatRule;				// （业财）统计因子规则表
	std::string         m_fieldPeriod;				// 源表账期字段名
	std::string         m_fieldCity;				// 源表地市字段名
	std::string         m_fieldBatch;				// 源表批次字段名

protected:
	int                 m_ycSeqID;					// 任务流水号
	std::string         m_taskCity;					// 任务地市
	std::vector<YCInfo> m_vecYCInfo;				// 业财稽核因子规则信息
};

