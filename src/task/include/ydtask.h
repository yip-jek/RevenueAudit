#pragma once

#include <map>
#include <set>
#include "ydstruct.h"
#include "task.h"

class YDTaskDB2;

// 一点稽核-任务调度
class YDTask : public Task
{
	friend class TaskFactory;

public:
	// 任务错误代码
	enum YD_TASK_ERROR
	{
		YDTERR_INIT_CONN     = -11000001,			// 初始化数据库连接失败
		YDTERR_INIT          = -11000002,			// 初始化失败
		YDTERR_HDL_ANA_TASK  = -11000003,			// 处理分析任务失败
		YDTERR_HDL_ETL_TASK  = -11000004,			// 处理采集任务失败
		YDTERR_CREATE_TASK   = -11000005,			// 下发任务失败
		YDTERR_BUILD_NEWTASK = -11000006,			// 创建新任务失败
	};

protected:
	explicit YDTask(base::Config& cfg);
	virtual ~YDTask();

public:
	// 版本号
	virtual std::string Version();

protected:
	// 载入配置
	virtual void LoadConfig() throw(base::Exception);

	// 初始化
	virtual void Init() throw(base::Exception);

	// 日志文件名前缀
	virtual std::string LogPrefix() const;

	// 输出配置信息
	void OutputConfiguration();

	// 确认退出？
	virtual bool ConfirmQuit();

	// 获取新任务
	virtual void GetNewTask() throw(base::Exception);

	// 获取新的任务日程
	void GetNewTaskSche();

	// 删除不存在或者没有激活的任务
	void DelUnavailableTask();

	// 不获取任务
	virtual void GetNoTask() throw(base::Exception);

	// 输出任务信息
	virtual void ShowTasksInfo();

	// 处理分析任务
	virtual void HandleAnaTask() throw(base::Exception);

	// 处理采集任务
	virtual void HandleEtlTask() throw(base::Exception);

	// 采集是否成功？
	bool IsEtlSucceeded(const TaskScheLog& ts_log) const;

	// 创建新任务
	virtual void BuildNewTask() throw(base::Exception);

	// 执行新任务
	int PerformNewTask(const base::SimpleTime& st_now, RATask& rat, bool task_continue);

	// 下发任务
	void CreateTask(TaskScheLog& ts_log);

	// 检查：是否有同序号或者同指标的任务在运行？
	bool CheckSameKindRunningTask(const RATask& rat, std::string* pMsg = NULL);

	// 是否超过最小时间间隔？
	bool IsOverMinTimeInterval(const base::SimpleTime& st_time) const;

	// 任务完成
	virtual void FinishTask() throw(base::Exception);

private:
	// 释放数据库连接
	void ReleaseDB();

	// 初始化数据库连接
	void InitConnect() throw(base::Exception);

	// 从任务暂停队列中删除指定序号的任务
	bool DelTaskPauseWithSEQ(const int& seq);

private:
	int              m_minRunTimeInterval;			// 任务运行的最小时间间隔（分钟）
	int              m_maxTaskScheLogID;			// 最大的任务日程日志ID
	int              m_taskFinished;				// 任务完成数统计
	base::SimpleTime m_stLastTaskBuild;				// 上一次创建任务的时间

private:
	DBInfo     m_dbinfo;			// 数据库信息
	YDTaskDB2* m_pTaskDB2;			// 数据库连接

private:
	std::string              m_hiveAgentPath;		// Hive代理的路径
	std::string              m_binVer;				// 程序版本：DEBUG or RELEASE
	std::string              m_binAcquire;			// 采集程序（带路径）
	std::string              m_modeAcquire;			// 采集程序模式
	std::string              m_cfgAcquire;			// 采集配置文件（带路径）
	std::string              m_binAnalyse;			// 分析程序（带路径）
	std::string              m_modeAnalyse;			// 分析程序模式
	std::string              m_cfgAnalyse;			// 分析配置文件（带路径）
	std::string              m_etlStateSuccess;		// 采集成功状态
	std::vector<std::string> m_vecEtlIgnoreError;	// 采集忽略的错误列表

private:
	std::string m_tabTaskSche;				// 任务日程表
	std::string m_tabTaskScheLog;			// 任务日程日志表
	std::string m_tabKpiRule;				// 指标规则表
	std::string m_tabEtlRule;				// 采集规则表

private:
	std::map<int, TaskSchedule> m_mTaskSche;			// 任务日程
	std::map<int, TaskSchedule> m_mTaskSche_bak;		// 任务日程备份
	std::map<int, RATask>       m_mTaskWait;			// 任务等待队列
	std::list<RATask>         	m_listTaskPause;		// 任务暂停队列
	std::map<int, RATask>       m_mEtlTaskRun;			// 采集运行任务队列
	std::map<int, RATask>       m_mAnaTaskRun;			// 分析运行任务队列
	std::map<int, RATask>       m_mTaskEnd;				// 任务完成队列
	std::set<int>               m_sDelAfterRun;			// 运行完需要删除的任务ID
};

