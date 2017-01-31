#pragma once

#include "dbinfo.h"
#include "pubstr.h"

// 任务日程
struct TaskSchedule
{
	TaskSchedule(): seq_id(0), activate('\0')
	{}

	// 是否已激活
	bool IsActivated() const
	{ return ('1' == activate); }

	// 是否为临时任务
	bool IsTemporaryTask() const
	{ return (base::PubStr::TrimUpperB(task_type) == "T"); }

	// 是否为常驻任务
	bool IsPermanentTask() const
	{ return (base::PubStr::TrimUpperB(task_type) == "P"); }

	int         seq_id;					// 序号
	char        activate;				// 是否激活（有效）
	std::string task_type;				// 任务类型
	std::string kpi_id;					// 指标 ID
	std::string task_cycle;				// 任务周期
	std::string task_state;				// 任务状态
	std::string task_state_desc;		// 任务状态描述
	std::string expiry_date_start;		// 有效期开始
	std::string expiry_date_end;		// 有效期结束
};

// 任务周期
struct TaskCycle
{
};

// 稽核任务
struct RATask
{
	// 任务类型
	enum TASK_TYPE
	{
		TTYPE_U = 0,		// 未知任务（预设值）
		TTYPE_T = 1,		// 临时任务
		TTYPE_P = 2,		// 常驻任务
	};

	RATask(): seq_id(0), type(TTYPE_U)
	{}

	int         seq_id;					// 序号
	TASK_TYPE   type;					// 任务类型
	std::string kpi_id;					// 指标 ID
	TaskCycle   cycle;					// 任务周期
	long long   exprry_date_start;		// 有效期开始
	long long   expiry_date_end;		// 有效期结束
	std::vector<
};

