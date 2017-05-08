#pragma once

#include "alarmmanager.h"

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

private:
	std::string m_tabAlarmRequest;				// 告警请求表
	std::string m_tabAlarmThreshold;			// 告警阈值表
	std::string m_tabAlarmInfo;					// 告警信息表
};

