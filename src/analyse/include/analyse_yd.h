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
	virtual void GetExtendParaTaskInfo(VEC_STRING& vec_str) throw(base::Exception);

	// 分析源数据，生成结果数据
	virtual void AnalyseSourceData() throw(base::Exception);

private:
	// 获取维度索引位置
	void GetDimIndex();

	// 尝试从分析表达式中获取指定渠道标识
	bool TryGetTheChannel(VEC_STRING& vec_channel);

	// 只保留指定渠道的报表数据
	void KeepChannelDataOnly(const VEC_STRING& vec_channel);

	// 筛选需要补全的地市
	void FilterTheMissingCity(const VEC_STRING& vec_channel, std::map<std::string, std::set<std::string> >& mapset_city);

	// 补全地市
	void MakeCityCompleted(const std::map<std::string, std::set<std::string> >& mapset_city, int group_size);

	// 生成告警请求
	void AlarmRequest();

private:
	int         m_taskScheLogID;			// 任务日程日志ID
	std::string m_tabTaskScheLog;			// 任务日程日志表
	std::string m_tabAlarmRequest;			// 告警请求表

private:
	int m_dimEtlDayIndex;				// 采集时间维度索引位置
	int m_dimNowDayIndex;				// 当前时间维度索引位置
	int m_dimRegionIndex;				// 地市维度索引位置
	int m_dimChannelIndex;				// 渠道维度索引位置
};

