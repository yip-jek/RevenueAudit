#pragma once

#include <string>
#include "exception.h"
#include "config.h"

namespace base
{

class Log;
class BaseDB2;
class BaseHiveThrift;

class BaseFrameApp
{
public:
	BaseFrameApp();
	virtual ~BaseFrameApp();

public:
	// 版本信息
	virtual const char* Version();

	// 配置输入参数
	virtual void SetArgv(char** pp_arg);

	// 载入配置信息
	virtual void LoadConfig() throw(Exception) = 0;

	// 获取配置文件名(含路径)
	virtual std::string GetConfigFile();

	// 获取日志路径
	virtual std::string GetLogPathConfig();

	// 初始化
	virtual void Init() throw(Exception) = 0;

	// 任务执行
	virtual void Run() throw(Exception) = 0;

protected:
	Config			m_cfg;
	Log*			m_pLog;
	char**			m_ppArgv;

protected:
	BaseDB2*		m_pDB2;
	BaseHiveThrift*	m_pHiveThrift;
};

}	// namespace base

extern base::BaseFrameApp* g_pApp;
