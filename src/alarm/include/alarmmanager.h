#pragma once

#include "baseframeapp.h"
#include "dbinfo.h"

// 告警管理
class AlarmManager : public base::BaseFrameApp
{
public:
	AlarmManager();
	virtual ~AlarmManager();

	// 状态
	enum AM_STATE
	{
		AMS_BEGIN,				// 状态：开始（初始值）
		AMS_RUNNING,			// 状态：运行
		AMS_END,				// 状态：结束（收到退出信号）
		AMS_QUIT				// 状态：退出
	};

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

	// 输出扩展配置信息
	virtual void OutputExtendedConfig() = 0;

	// 是否确认退出
	virtual bool ConfirmQuit() = 0;

	// 告警处理
	virtual void AlarmProcessing() throw(base::Exception) = 0;

private:
	// 载入基础配置
	void LoadBasicConfig() throw(base::Exception);

	// 输出基础配置信息
	void OutputBasicConfig();

	// 是否保持运行
	bool Running();

protected:
	DBInfo m_dbinfo;					// 数据库信息

private:
	int      m_timeInterval;			// 时间间隔
	AM_STATE m_AMState;					// 告警管理状态
};

