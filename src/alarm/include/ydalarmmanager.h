#pragma once

#include "alarmmanager.h"
#include <vector>
#include <map>

class YDAlarmDB;
struct YDAlarmReq;
struct YDAlarmThreshold;

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

	// 采集分析数据
	void DataCollectionAnalysis();

	// 产生告警信息
	void YieldAlarmInformation();

	// 生成告警短信
	void ProduceAlarmMessage();

	// 告警数据采集
	double CollectData(const YDAlarmReq& req);

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
	std::vector<YDAlarmReq>       m_vAlarmReq;			// 告警请求列表
	std::vector<YDAlarmThreshold> m_vAlarmThres;		// 告警阈值列表
	std::map<int, double>         m_mAlarmSrcData;		// 告警源数据列表
};

