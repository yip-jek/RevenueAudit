#pragma once

#include "exception.h"

class TaskFactory;

class Task
{
private:
	friend class TaskFactory;

	Task(const Task& t);
	Task& operator = (const Task& t);

protected:
	explicit Task(base::Config& cfg);
	virtual ~Task();

public:
	// 任务错误代码
	enum TASK_ERROR
	{
		TERROR_CHECK             = -10000001,					// 检查失败
		TERROR_CREATE_TASK       = -10000002,					// 下发任务失败
		TERROR_ETLTIME_TRANSFORM = -10000003,					// 采集时间转换失败
		TERROR_UPD_TASK_REQ      = -10000004,					// 更新任务请求失败
		TERROR_HDL_ETL_TASK      = -10000005,					// 处理采集任务失败
		TERROR_IS_PROC_EXIST     = -10000006,					// 查看进程是否存在失败
	};

public:
	// 版本号
	virtual std::string Version();

	// 执行
	void Run() throw(base::Exception);

protected:
	// 初始化
	virtual void Init() throw(base::Exception) = 0;

	// 处理任务
	virtual void DealTasks() throw(base::Exception);

	// 获取新任务
	virtual void GetNewTask() throw(base::Exception) = 0;

	// 输出任务当前状态
	virtual void ShowTask() throw(base::Exception) = 0;

	// 执行任务
	virtual void ExecuteTask() throw(base::Exception);

	// 任务完成
	virtual void FinishTask() throw(base::Exception) = 0;

	// 处理分析任务
	virtual void HandleAnaTask() throw(base::Exception) = 0;

	// 处理采集任务
	virtual void HandleEtlTask() throw(base::Exception) = 0;

	// 创建新任务
	virtual void BuildNewTask() throw(base::Exception) = 0;

	// 查看进程是否存在
	virtual bool IsProcessAlive(long long proc_task_id) throw(base::Exception);

	// 生成新的任务ID
	long long GenerateTaskID();

	// 采集时间转换
	std::string EtlTimeTransform(const std::string& cycle) throw(base::Exception);

protected:
	base::Config* m_pCfg;					// 配置文件
	base::Log*    m_pLog;					// 日志输出
	int           m_TIDAccumulator;			// 任务ID的累加值
	long          m_waitSeconds;			// 处理时间间隔（单位：秒）
};

