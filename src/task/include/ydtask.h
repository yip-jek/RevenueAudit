#pragma once

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
		YDTERR_INIT_CONN    = -11000001,			// 初始化数据库连接失败
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

	// 确认退出？
	virtual bool ConfirmQuit();

	// 获取新任务
	virtual void GetNewTask() throw(base::Exception);

	// 输出任务信息
	virtual void ShowTasksInfo();

	// 处理分析任务
	virtual void HandleAnaTask() throw(base::Exception);

	// 处理采集任务
	virtual void HandleEtlTask() throw(base::Exception);

	// 创建新任务
	virtual void BuildNewTask() throw(base::Exception);

	// 任务完成
	virtual void FinishTask() throw(base::Exception);

private:
	// 释放数据库连接
	void ReleaseDB();

	// 初始化数据库连接
	void InitConnect() throw(base::Exception);

private:
	DBInfo     m_dbinfo;			// 数据库信息
	YDTaskDB2* m_pTaskDB2;			// 数据库连接

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
	std::string m_tabTaskSche;				// 任务日程表
	std::string m_tabKpiRule;				// 指标规则表
	std::string m_tabEtlRule;				// 采集规则表

};

