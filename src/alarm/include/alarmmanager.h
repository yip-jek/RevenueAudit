#pragma once

#include "baseframeapp.h"

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

	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();

	// 初始化
	virtual void Init() throw(base::Exception);

	// 任务执行
	virtual void Run() throw(base::Exception);

	// 任务结束（资源回收）
	virtual void End(int err_code, const std::string& err_msg = std::string()) throw(base::Exception);

private:
};

