#pragma once

#include "alarmmanager.h"
#include <vector>

class YDAlarmDB;
struct YDAlarmReq;
struct YDAlarmThreshold;
struct YDAlarmData;
struct YDAlarmInfo;

// 一点稽核告警管理
class YDAlarmManager : public AlarmManager
{
public:
	YDAlarmManager();
	virtual ~YDAlarmManager();

public:
	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();

	// 初始化
	virtual void Init() throw(base::Exception);

protected:
	// 载入扩展配置
	virtual void LoadExtendedConfig() throw(base::Exception);

	// 是否确认退出
	virtual bool ConfirmQuit();

	// 告警处理
	virtual void AlarmProcessing() throw(base::Exception);

private:
	// 释放数据库连接
	void ReleaseDBConnection();

	// 初始化数据库连接
	void InitDBConnection() throw(base::Exception);

	// 响应告警请求
	// 有新请求则返回true，否则返回false
	bool ResponseAlarmRequest();

	// 数据分析
	void DataAnalysis();

	// 告警生成
	void GenerateAlarm();

	// 处理告警请求
	void HandleRequest();

	// 更新告警阈值
	void UpdateAlarmThreshold();

	// 告警数据采集
	void CollectData();

	// 组织 SQL 条件
	std::string AssembleSQLCondition(const YDAlarmReq& req);

	// 告警判断
	void DetermineAlarm();

	// 获取对比阈值
	bool GetAlarmThreshold(const YDAlarmData& alarm_data, YDAlarmThreshold*& pThreshold);

	// 是否达到告警阈值
	bool IsReachThreshold(const YDAlarmData& alarm_data, const YDAlarmThreshold& alarm_threshold);

	// 产生告警信息
	void MakeAlarmInformation(const YDAlarmData& alarm_data, const YDAlarmThreshold& alarm_threshold);

	// 生成告警短信
	void ProduceAlarmMessage();

private:
	std::string m_tabAlarmRequest;				// 告警请求表
	std::string m_tabAlarmThreshold;			// 告警阈值表
	std::string m_tabAlarmInfo;					// 告警信息表
	std::string m_tabSrcData;					// 数据源表

private:
	std::string m_alarmMsgFilePath;				// 告警短信文件路径
	std::string m_alarmMsgFileFormat;			// 告警短信文件格式

private:
	YDAlarmDB*                    m_pAlarmDB;
	std::vector<YDAlarmReq>       m_vAlarmReq;				// 告警请求列表
	std::vector<YDAlarmThreshold> m_vAlarmThreshold;		// 告警阈值列表
	std::vector<YDAlarmData>      m_vAlarmData;				// 告警数据列表
	std::vector<YDAlarmInfo>      m_vAlarmInfo;				// 告警信息列表
};

