#pragma once

#include "sectimer.h"
#include "tasktempfile.h"

// 任务状态切换
class TaskStatusSwitch
{
public:
	TaskStatusSwitch();
	virtual ~TaskStatusSwitch();

	static const long S_CHK_TEMPFILE_INTERVAL = 60;			// 检查临时文件状态的时间间隔（单位：秒）
	static const int  S_FREEZE_HOUR           = 23;			// 冻结的小时时间
	static const int  S_FREEZE_MINUTE         = 30;			// 冻结的分钟时间

public:
	// 初始化
	// temp_path 为临时目录
	bool Init(const std::string& temp_path);

	// 检查状态
	void Check();

	// 获取当前状态
	bool GetPauseState()  const { return m_pause ; }		// 暂停状态
	bool GetFrozenState() const { return m_frozen; }		// 冻结状态

private:
	// 清除状态
	void Clear();

	// 检查冻结状态
	void CheckFrozenState();

private:
	SecTimer     m_timer;			// 计时器
	bool         m_pause;			// 暂停状态
	bool         m_frozen;			// 冻结状态
	TaskTempFile m_tmpFile;			// 临时文件
};

