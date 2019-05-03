#pragma once

#include "exception.h"
#include "sectimer.h"
#include "taskstatusswitch.h"
#include "ycstruct.h"

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
		TERR_INIT_BASE_CFG     = -10000001,			// 初始化基础任务配置失败
		TERR_DEAL_TASKS        = -10000002,			// 处理任务失败
		TERR_ETLTIME_TRANSFORM = -10000003,			// 采集时间转换失败
		TERR_IS_PROC_EXIST     = -10000004,			// 查看进程是否存在失败
	};

public:
	// 版本号
	virtual std::string Version();

	// 日志文件名前缀
	virtual std::string LogPrefix() const = 0;

	// 执行
	void Run();

protected:
	// 初始化基础任务配置
	virtual void InitBaseConfig();

	// 载入配置
	virtual void LoadConfig() = 0;

	// 初始化
	virtual void Init() = 0;

	// 是否继续运行
	virtual bool Running();

	// 确认退出？
	virtual bool ConfirmQuit() = 0;

	// 检查任务状态
	virtual void CheckTaskStatus();

	// 处理任务
	virtual void DealTasks();

	// 获取任务
	virtual void GetTasks();

	// 获取新任务
	virtual void GetNewTask() = 0;

	// 不获取任务
	virtual void GetNoTask() = 0;

	// 定时输出任务当前状态
	virtual void ShowTask();

	// 输出任务信息
	virtual void ShowTasksInfo() = 0;

	// 执行任务
	virtual void ExecuteTask();

	// 任务完成
	virtual void FinishTask() = 0;

	// 处理分析任务
	virtual void HandleAnaTask() = 0;

	// 处理采集任务
	virtual void HandleEtlTask() = 0;

	// 创建新任务
	virtual void BuildNewTask() = 0;

	// 查看进程是否存在
	virtual bool IsProcessAlive(long long proc_task_id);
    virtual bool IsProcessAlive(const TaskInfo& task_info, const TaskReqInfo& tri, bool IsGDAudiKPIType);

	// 生成新的任务ID
	long long GenerateTaskID();

	// 采集时间转换
	std::string EtlTimeTransform(const std::string& cycle);

    virtual void GetUndoneTask();

private:
	std::string FetchPipeBuffer(const std::string& cmd);

protected:
	base::Config*    m_pCfg;					// 配置文件
	base::Log*       m_pLog;					// 日志输出
	TASK_STATE       m_state;					// 任务状态
	int              m_TIDAccumulator;			// 任务ID的累加值
	long             m_waitSeconds;				// 处理时间间隔（单位：秒）
	long             m_showSeconds;				// 任务日志输出的时间间隔（单位：秒）
	SecTimer         m_showTimer;				// 任务日志输出计时
	TaskStatusSwitch m_tsSwitch;				// 任务状态切换
	bool             m_taskContinue;			// 任务是否继续的标志
};

