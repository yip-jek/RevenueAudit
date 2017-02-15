#pragma once

#include "acquire.h"

// 话单稽核-采集模块
class Acquire_HD : public Acquire
{
public:
	Acquire_HD();
	virtual ~Acquire_HD();

	static const char* const S_HD_ETLRULE_TYPE;				// 话单稽核-采集规则类型

public:
	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();

protected:
	// 获取后续参数任务信息
	virtual void GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception);

	// 检查采集任务信息
	virtual void CheckTaskInfo() throw(base::Exception);

	// 进行数据采集
	virtual void DoDataAcquisition() throw(base::Exception);

	// 重建Hive目标表
	// 返回：目标表的字段数
	virtual int RebuildHiveTable() throw(base::Exception);

	// 检查源表是否存在
	virtual void CheckSourceTable(bool hive) throw(base::Exception);

	// 分析采集任务规则，生成采集SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	virtual void TaskInfo2Sql(std::vector<std::string>& vec_sql, bool hive) throw(base::Exception);
};

