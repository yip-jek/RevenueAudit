#pragma once

#include "baseframeapp.h"
#include "dbinfo.h"

// 告警管理
class AlarmManager : public base::BaseFrameApp
{
public:
	AlarmManager();
	virtual ~AlarmManager();

public:
	// 版本信息
	virtual const char* Version();

	// 载入配置信息
	virtual void LoadConfig() throw(base::Exception);

	// 初始化
	virtual void Init() throw(base::Exception);

	// 任务执行
	virtual void Run() throw(base::Exception);

	// 任务结束（资源回收）
	virtual void End(int err_code, const std::string& err_msg = std::string()) throw(base::Exception);

protected:
	// 载入扩展配置
	virtual void LoadExtendedConfig() throw(base::Exception) = 0;

private:
	// 载入基础配置
	void LoadBasicConfig() throw(base::Exception);

protected:
	int m_timeInterval;			// 时间间隔

protected:
	DBInfo m_dbinfo;			// 数据库信息
};

