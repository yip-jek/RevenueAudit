#pragma once

#include "dbinfo.h"
#include "pubstr.h"
#include "pubtime.h"
#include "simpletime.h"

// 任务日程
class TaskSchedule
{
public:
	TaskSchedule();

	friend bool operator == (const TaskSchedule& ts1, const TaskSchedule& ts2);
	friend bool operator != (const TaskSchedule& ts1, const TaskSchedule& ts2);

public:
	// 是否为临时任务
	bool IsTemporaryTask() const;

	// 是否为常驻任务
	bool IsPermanentTask() const;

public:
	int         seq_id;					// 序号
	std::string task_type;				// 任务类型
	std::string kpi_id;					// 指标 ID
	std::string task_cycle;				// 任务周期
	std::string etl_time;				// 采集时间
	std::string expiry_date_start;		// 有效期开始
	std::string expiry_date_end;		// 有效期结束
};


////////////////////////////////////////////////////////////////////////////////
// 任务周期
class TaskCycle
{
public:
	TaskCycle();

	static const int ANY_TIME = -1;

public:
	// 是否有效
	bool IsValid() const;

	// 设置周期
	bool Set(const std::string& tc);

	// 是否到时间点
	bool IsCycleTimeUp();

private:
	void Clear();

private:
	bool valid;			// 是否有效
	int  year;			// 年
	int  mon;			// 月
	int  day;			// 日
	int  hour;			// 时
	int  min;			// 分
	int  sec;			// 秒
};

// 采集时间
class EtlTime
{
public:
	EtlTime();

public:
	// 设置采集时间
	bool SetTime(const std::string& time);

	// 是否有效
	bool IsValid() const;

	// 初始化
	void Init();

	// 获取下一个采集时间字串
	bool GetNext(std::string& etl);

private:
	base::PubTime::DATE_TYPE dt_type;		// 时间类型
	std::string              etl_time;		// 采集时间字串
	std::vector<int>         vecTime;		// 时间列表
	int                      currIndex;		// 当前列表中位置
};

// 任务日程日志
class TaskScheLog
{
public:
	TaskScheLog();

	static const char* const S_APP_TYPE_ETL;			// 采集程序类型
	static const char* const S_APP_TYPE_ANA;			// 分析程序类型

public:
	// 重置
	void Clear();

public:
	int         log_id;				// 序号
	std::string kpi_id;				// 指标 ID
	std::string sub_id;				// 子 ID （采集ID或者分析ID）
	std::string task_id;			// 任务 ID
	std::string task_type;			// 任务类型
	std::string etl_time;			// 采集时间
	std::string app_type;			// 程序类型
	std::string start_time;			// 开始时间
	std::string end_time;			// 结束时间
	std::string task_state;			// 任务状态
	std::string state_desc;			// 任务状态描述
	std::string remarks;			// 备注
};

// 稽核任务
class RATask
{
public:
	RATask();

	// 任务类型
	enum TASK_TYPE
	{
		TTYPE_U = 0,		// 未知任务（预设值）
		TTYPE_T = 1,		// 临时任务
		TTYPE_P = 2,		// 常驻任务
	};

public:
	// 载入任务日程
	bool LoadFromTaskSche(const TaskSchedule& ts);

public:
	int                      seq_id;					// 序号
	TASK_TYPE                type;						// 任务类型
	std::string              kpi_id;					// 指标 ID
	TaskCycle                cycle;						// 任务周期
	EtlTime                  etl_time;					// 采集时间
	long long                expiry_date_start;			// 有效期开始时间
	long long                expiry_date_end;			// 有效期结束时间
	std::string              task_starttime;			// 任务开始时间
	std::string              task_finishtime;			// 任务结束时间
	std::vector<TaskScheLog> vecEtlTasks;				// 采集任务日志
	TaskScheLog              tslogAnaTask;				// 分析任务日志
};

