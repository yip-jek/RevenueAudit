#pragma once

#include <map>
#include "analyse.h"

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

	// 统计因子转换
	void TransYCStatFactor(std::map<std::string, double>& map_factor) throw(base::Exception);

	// 生成业财稽核结果数据
	void GenerateYCResultData(std::map<std::string, double>& map_factor) throw(base::Exception);

	// 计算组合因子的维度值
	double CalcYCComplexFactor(std::map<std::string, double>& map_factor, const std::string& cmplx_factr_fmt) throw(base::Exception);

	// 告警判断: 如果达到告警阀值，则生成告警
	virtual void AlarmJudgement() throw(base::Exception);

	// 更新维度取值范围
	virtual void UpdateDimValue();

protected:
	int                     m_ycSeqID;				// 任务流水号
	std::string             m_tabYCTaskReq;			// （业财）任务请求表
	std::vector<YCStatInfo> m_vecYCSInfo;			// 业财稽核因子规则信息
};

