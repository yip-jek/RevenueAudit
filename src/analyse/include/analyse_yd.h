#pragma once

#include <set>
#include "analyse.h"

// 一点稽核-分析模块
class Analyse_YD : public Analyse
{
public:
	Analyse_YD();
	virtual ~Analyse_YD();

	static const char* const S_CHANNEL_MARK;			// 渠道标识

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

	// 分析源数据，生成结果数据
	virtual void AnalyseSourceData() throw(base::Exception);

private:
	// 尝试从分析表达式中获取指定渠道标识
	std::string TryGetTheChannel();

	// 只保留指定渠道的报表数据
	void KeepChannelDataOnly(const std::string& channel);

	// 筛选需要补全的地市
	void FilterTheMissingCity(std::set<std::string>& set_city, int dim_region_index);

	// 补全地市
	void MakeCityCompleted(const std::set<std::string>& set_city, const std::string& channel, int dim_size, int col_size);

	// 生成告警请求
	void AlarmRequest();

private:
	int         m_taskScheLogID;			// 任务日程日志ID
	std::string m_tabTaskScheLog;			// 任务日程日志表
	std::string m_tabAlarmRequest;			// 告警请求表
};

