#pragma once

#include "acquire.h"

class SQLExtendConverter;

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

	// 进行数据采集
	virtual void DoDataAcquisition() throw(base::Exception);

	// 采集业财稽核数据
	void YCDataAcquisition() throw(base::Exception);

	// 检查源表是否存在
	virtual void CheckSourceTable(bool hive) throw(base::Exception);

	// 生成hdfs临时文件名
	virtual std::string GeneralHdfsFileName();

	// 生成采集时间
	virtual void GenerateEtlDate(const std::string& date_fmt) throw(base::Exception);

	// 分析处理业财稽核统计因子规则
	void HandleYCInfo() throw(base::Exception);

	// 创建 SQL 扩展转换
	void CreateSqlExtendConv();

	// 释放 SQL 扩展转换
	void ReleaseSqlExtendConv();

	// SQL 扩展转换
	void SQLExtendConvert(std::string& sql);

	// 补全业财采集结果数据
	void MakeYCResultComplete(const std::string& dim, const int& fields, std::vector<std::vector<std::string> >& vec_result);

protected:
	std::string         m_tabYCTaskReq;				// 任务请求表
	std::string         m_tabStatRule;				// 统计因子规则表
	std::string         m_tabReportStat;			// 稽核报表状态表
	std::string         m_tabDictCity;				// 地市统一编码表
	std::string         m_fieldPeriod;				// 源表账期字段名
	std::string         m_fieldCity;				// 源表地市字段名
	std::string         m_fieldBatch;				// 源表批次字段名

protected:
	int                 m_ycSeqID;					// 任务流水号
	std::string         m_taskCity;					// 任务地市（编码）
	std::string         m_taskCityCN;				// 任务地市（中文名称）
	std::vector<YCInfo> m_vecYCInfo;				// 业财稽核因子规则信息
	SQLExtendConverter* m_pSqlExConv;				// SQL 扩展转换
};

