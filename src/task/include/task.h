#pragma once

#include "exception.h"

namespace base
{
class Config;
class Log;
}

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
	// 任务运行状态
	enum TASK_STATE
	{
		TS_BEGIN   = 0,				// 状态：开始
		TS_RUNNING = 1,				// 状态：运行中
		TS_END     = 2,				// 状态：结束（收到退出信号）
		TS_QUIT    = 3,				// 状态：退出
	};

	// 任务错误代码
	enum TASK_ERROR
	{
		TERROR_CHECK             = -10000001,					// 检查失败
		TERROR_DEAL_TASKS        = -10000002,					// 处理任务失败
		TERROR_CREATE_TASK       = -10000003,					// 下发任务失败
		TERROR_ETLTIME_TRANSFORM = -10000004,					// 采集时间转换失败
		TERROR_UPD_TASK_REQ      = -10000005,					// 更新任务请求失败
		TERROR_HDL_ETL_TASK      = -10000006,					// 处理采集任务失败
		TERROR_IS_PROC_EXIST     = -10000007,					// 查看进程是否存在失败
	};

public:
	// 版本号
	virtual std::string Version();

	// 执行
	void Run() throw(base::Exception);

protected:
	// 初始化基础任务配置
	virtual void InitBaseConfig() throw(base::Exception);

	// 载入配置
	virtual void LoadConfig() throw(base::Exception) = 0;

	// 初始化
	virtual void Init() throw(base::Exception) = 0;

	// 是否继续运行
	virtual bool Running();

	// 确认退出？
	virtual bool ConfirmQuit() = 0;

	// 处理任务
	virtual void DealTasks() throw(base::Exception);

	// 获取新任务
	virtual void GetNewTask() throw(base::Exception) = 0;

	// 定时输出任务当前状态
	virtual void ShowTask();

	// 输出任务信息
	virtual void ShowTasksInfo() = 0;

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
	TASK_STATE    m_state;					// 任务状态
	int           m_TIDAccumulator;			// 任务ID的累加值
	long          m_waitSeconds;			// 处理时间间隔（单位：秒）
	long          m_showMaxTime;			// 任务日志输出的时间间隔（单位：秒）
	time_t        m_taskShowTime;			// 任务日志输出的计时（单位：秒）
};

