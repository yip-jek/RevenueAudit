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
		TERROR_CHECK       = -10000001,					// 检查失败
		TERROR_CREATE_TASK = -10000002,					// 下发任务失败
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

	// 执行任务
	void ExecuteTask();

	// 下发任务
	void CreateTask(const TaskInfo& t_info) throw(base::Exception);

	// 创建新任务
	void BuildNewTask();

	// 生成新的任务ID
	long long GenerateTaskID();

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
	std::string m_hiveAgentPath;			// Hive代理的路径
	std::string m_binAcquire;				// 采集程序（带路径）
	std::string m_cfgAcquire;				// 采集配置文件（带路径）
	std::string m_binAnalyse;				// 分析程序（带路径）
	std::string m_cfgAnalyse;				// 分析配置文件（带路径）

private:
	std::string m_tabTaskReq;				// 任务请求表
	std::string m_tabKpiRule;				// 指标规则表
	std::string m_tabEtlRule;				// 采集规则表

private:
	std::vector<TaskReqInfo>   m_vecNewTask;			// 新任务列表
	std::vector<TaskReqInfo>   m_vecEndTask;			// 已完成的任务列表
	std::map<int, TaskReqInfo> m_mTaskReqInfo;			// 执行的任务列表

	std::vector<int>           m_vecEtlSeq;				// 采集任务流水号列表
	std::vector<int>           m_vecAnaSeq;				// 分析任务流水号列表
};

