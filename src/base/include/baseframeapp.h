#pragma once

#include <string>
#include "exception.h"
#include "config.h"

namespace base
{

class Log;
class LogStrategy;
class BaseDB2;
class BaseJHive;

class BaseFrameApp
{
public:
	BaseFrameApp();
	virtual ~BaseFrameApp();

public:
	// 版本信息
	virtual const char* Version();

	// 设置TEST标志
	virtual void SetTestFlag(bool is_test);

	// 配置输入参数
	virtual void SetArgv(char** pp_arg);

	// 载入配置信息
	virtual void LoadConfig() = 0;

	// 获取配置文件名(含路径)
	virtual std::string GetConfigFile();

	// 导出日志配置
	virtual void ExportLogConfig(LogStrategy& ls);

	// 获取任务参数信息
	virtual std::string GetTaskParaInfo();

	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix() = 0;

	// 初始化
	virtual void Init() = 0;

	// 任务执行
	virtual void Run() = 0;

	// 任务结束（资源回收）
	virtual void End(int err_code, const std::string& err_msg = std::string()) = 0;

protected:
	bool       m_isTest;				// 是否为测试版本

protected:
	Config     m_cfg;
	Log*       m_pLog;
	char**     m_ppArgv;

protected:
	BaseDB2*   m_pDB2;
	BaseJHive* m_pHive;
};

}	// namespace base

