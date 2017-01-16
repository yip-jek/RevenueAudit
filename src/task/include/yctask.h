#pragma once

#include <map>
#include "struct.h"
#include "task.h"

namespace base
{
class Config;
class Log;
}

class TaskDB2;

// 任务调度
class YCTask : public Task
{
	friend class TaskFactory;

public:
	// 任务状态
	enum TS_TASK_STATE
	{
		TSTS_Unknown          = 0,				// 未知状态（预设值）
		TSTS_Start            = 1,				// 任务开始
		TSTS_EtlException     = 2,				// 采集异常
		TSTS_AnalyseBegin     = 3,				// 开始分析
		TSTS_AnalyseException = 4,				// 分析异常
		TSTS_End              = 5,				// 任务完成
	};

protected:
	explicit YCTask(base::Config& cfg);
	virtual ~YCTask();

public:
	// 版本号
	virtual std::string Version();

protected:
	// 初始化
	virtual void Init() throw(base::Exception);

	// 处理任务
	virtual void DealTasks() throw(base::Exception);

	// 获取新任务
	virtual void GetNewTask() throw(base::Exception);

	// 输出任务当前状态
	virtual void ShowTask() throw(base::Exception);

	// 处理分析任务
	virtual void HandleAnaTask() throw(base::Exception);

	// 处理采集任务
	virtual void HandleEtlTask() throw(base::Exception);

	// 创建新任务
	virtual void BuildNewTask() throw(base::Exception) = 0;

	// 任务完成
	virtual void FinishTask() throw(base::Exception) = 0;

private:
	// 释放数据库连接
	void ReleaseDB();

	// 载入配置
	void LoadConfig() throw(base::Exception);

	// 初始化数据库连接
	void InitConnect() throw(base::Exception);

	// 检查
	void Check() throw(base::Exception);

	// 更新任务请求
	void TaskRequestUpdate(TS_TASK_STATE ts, TaskReqInfo& task_req_info) throw(base::Exception);

	// 下发任务
	void CreateTask(const TaskInfo& t_info) throw(base::Exception);

	// 删除已存在的旧任务
	void RemoveOldTask(int task_seq);

	// 获取子规则ID
	// 成功返回true，失败返回false
	bool GetSubRuleID(const std::string& kpi_id, TaskInfo::TASK_TYPE t_type, std::string& sub_id);

private:
	long          m_showMaxTime;			// 任务日志输出的时间间隔（单位：秒）
	time_t        m_taskShowTime;			// 任务日志输出的计时（单位：秒）

private:
	DBInfo   m_dbInfo;				// 数据库信息
	TaskDB2* m_pTaskDB;				// 数据库连接

private:
	std::string m_hiveAgentPath;			// Hive代理的路径
	std::string m_binVer;					// 程序版本：DEBUG or RELEASE
	std::string m_binAcquire;				// 采集程序（带路径）
	std::string m_modeAcquire;				// 采集程序模式
	std::string m_cfgAcquire;				// 采集配置文件（带路径）
	std::string m_binAnalyse;				// 分析程序（带路径）
	std::string m_modeAnalyse;				// 分析程序模式
	std::string m_cfgAnalyse;				// 分析配置文件（带路径）

private:
	std::string m_stateTaskBeg;				// 任务开始
	std::string m_stateEtlException;		// 任务采集异常
	std::string m_stateAnaBeg;				// 任务开始分析
	std::string m_stateAnaException;		// 任务分析异常
	std::string m_stateTaskEnd;				// 任务完成
	std::string m_etlStateEnd;				// 采集完成状态
	std::string m_etlStateError;			// 采集异常状态
	std::string m_anaStateEnd;				// 分析完成状态
	std::string m_anaStateError;			// 分析异常状态

private:
	std::string m_tabTaskReq;				// 任务请求表
	std::string m_tabKpiRule;				// 指标规则表
	std::string m_tabEtlRule;				// 采集规则表

private:
	std::vector<TaskReqInfo>           m_vecNewTask;					// 新任务列表
	std::vector<TaskReqInfo>           m_vecEndTask;					// 已完成的任务列表
	std::map<int, TaskReqInfo>         m_mTaskReqInfo;					// 执行的任务列表
	std::map<std::string, KpiRuleInfo> m_mKpiRuleInfo;					// 指标规则信息列表

	std::vector<TaskInfo> m_vecEtlTaskInfo;					// 采集任务列表
	std::vector<TaskInfo> m_vecAnaTaskInfo;					// 分析任务列表
};

