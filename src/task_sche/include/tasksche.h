#pragma once

#include <map>
#include "exception.h"
#include "struct.h"

namespace base
{
class Config;
class Log;
}

class TaskDB2;

// 任务调度
class TaskSche
{
public:
	// 任务错误代码
	enum TASK_ERROR
	{
		TERROR_CHECK = -10000001,		// 检查失败
	};

public:
	TaskSche(base::Config& cfg);
	virtual ~TaskSche();

public:
	// 版本号
	const char* Version();

	// 执行
	void Do() throw(base::Exception);

private:
	// 释放数据库连接
	void ReleaseDB();

	// 初始化
	void Init() throw(base::Exception);

	// 载入配置
	void LoadConfig() throw(base::Exception);

	// 初始化数据库连接
	void InitConnect() throw(base::Exception);

	// 检查
	void Check() throw(base::Exception);

	// 处理任务
	void DealTasks() throw(base::Exception);

	// 获取新任务
	void GetNewTask();

	// 下发任务
	void ExecuteTask();

	// 任务完成
	void FinishTask();

private:
	base::Config* m_pCfg;					// 配置文件
	base::Log*    m_pLog;					// 日志输出
	long          m_waitSeconds;			// 处理时间间隔（单位：秒）

private:
	DBInfo   m_dbInfo;				// 数据库信息
	TaskDB2* m_pTaskDB;				// 数据库连接

private:
	std::string m_tabTaskReq;				// 任务请求表
	std::string m_tabRaKpi;					// 指标规则表
	std::string m_tabEtlRule;				// 采集规则表

private:
	std::map<int, TaskReqInfo> m_mTaskReqInfo;			// 任务列表
};

