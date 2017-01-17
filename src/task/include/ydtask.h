#pragma once

#include "task.h"

// 一点稽核-任务调度
class YDTask : public Task
{
	friend class TaskFactory;

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
};

