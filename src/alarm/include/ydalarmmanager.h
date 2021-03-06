#pragma once

#include <map>
#include <vector>
#include "alarmmanager.h"
#include "sectimer.h"

class YDAlarmDB;
class YDAlarmFile;
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
	virtual void Init();

protected:
	// 载入扩展配置
	virtual void LoadExtendedConfig();

	// 输出扩展配置信息
	virtual void OutputExtendedConfig();

	// 是否确认退出
	virtual bool ConfirmQuit();

	// 告警处理
	virtual void AlarmProcessing();

private:
	// 释放数据库连接
	void ReleaseDBConnection();

	// 释放告警短信文件
	void ReleaseAlarmSMSFile();

	// 初始化告警日志输出计时
	void InitAlarmShowTime();

	// 初始化数据库连接
	void InitDBConnection();

	// 初始化告警短信文件
	void InitAlarmSMSFile();

	// 响应告警请求
	// 有新请求则返回true，否则返回false
	bool ResponseAlarmRequest();

	// 数据分析
	void DataAnalysis();

	// 告警生成
	void GenerateAlarm();

	// 输出告警状态
	void ShowAlarmState();

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
	std::string ProduceAlarmMessage(const YDAlarmInfo& alarm_info);

	// 更新告警请求状态
	void UpdateRequestStatus();

private:
	int         m_totalAlarmRequests;			// 总处理告警请求数
	int         m_alarmShowTime;				// 告警日志输出时间间隔（单位：秒）
	SecTimer    m_stAlarmShow;					// 告警日志输出计时器

private:
	std::string m_tabAlarmRequest;				// 告警请求表
	std::string m_tabAlarmThreshold;			// 告警阈值表
	std::string m_tabAlarmInfo;					// 告警信息表
	std::string m_tabSrcData;					// 数据源表
	std::string m_alarmMsgFilePath;				// 告警短信文件路径
	std::string m_alarmMsgFileFormat;			// 告警短信文件格式
	int         m_alarmMsgFileMaxLine;			// 告警短信文件最大行数

private:
	YDAlarmDB*                    m_pAlarmDB;
	YDAlarmFile*                  m_pAlarmSMSFile;			// 告警短信文件
	std::map<int, YDAlarmReq>     m_mAlarmReq;				// 告警请求列表
	std::vector<YDAlarmThreshold> m_vAlarmThreshold;		// 告警阈值列表
	std::vector<YDAlarmData>      m_vAlarmData;				// 告警数据列表
	std::vector<YDAlarmInfo>      m_vAlarmInfo;				// 告警信息列表
};

