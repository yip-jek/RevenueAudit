#pragma once

#include "alarmmanager.h"

class YDAlarmDB;

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

private:
	std::string m_tabAlarmRequest;				// 告警请求表
	std::string m_tabAlarmThreshold;			// 告警阈值表
	std::string m_tabAlarmInfo;					// 告警信息表

private:
	YDAlarmDB*  m_pAlarmDB;
};

