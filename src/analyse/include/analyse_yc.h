#pragma once

#include <map>
#include "analyse.h"
#include "ycstatfactor.h"

// 业财稽核-分析模块
class Analyse_YC : public Analyse
{
public:
	Analyse_YC();
	virtual ~Analyse_YC();

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

	// 解析分析规则，生成Hive取数逻辑
	virtual void AnalyseRules(std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 获取任务信息
	virtual void FetchTaskInfo() throw(base::Exception);

	// 分析业财稽核源数据，生成结果数据
	virtual void AnalyseSourceData() throw(base::Exception);

	// 生成最新的批次
	void GenerateNewBatch();

	// 统计因子转换
	void ConvertStatFactor() throw(base::Exception);

	// 生成业财稽核结果数据
	void GenerateResultData() throw(base::Exception);

	// 结果数据入库 [DB2]
	virtual void StoreResult() throw(base::Exception);

	// 入库差异汇总结果数据
	void StoreDiffSummaryResult() throw(base::Exception);

	// 登记稽核记录日志
	void RecordStatisticsLog();

	// 删除采集目标表
	void DropEtlTargetTable();

	// 告警判断: 如果达到告警阀值，则生成告警
	virtual void AlarmJudgement() throw(base::Exception);

	// 更新维度取值范围
	virtual void UpdateDimValue();

protected:
	std::string  m_tabYCTaskReq;			// （业财）任务请求表
	std::string  m_tabStatRule;				// （业财）统计因子规则表
	std::string  m_tabStatLog;				// （业财）稽核记录日志表
	std::string  m_fieldPeriod;				// 账期字段名
	std::string  m_fieldCity;				// 地市字段名
	std::string  m_fieldBatch;				// 批次字段名

protected:
	int          m_ycSeqID;					// 任务流水号
	int          m_statBatch;				// 统计结果的批次
	std::string  m_taskCity;				// 任务地市
	YCStatFactor m_statFactor;				// 稽核规则因子类

protected:
	std::vector<std::vector<std::string> > m_vec2DiffSummary;			// 差异汇总结果数据
};

