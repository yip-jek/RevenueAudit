#pragma once

#include "dbinfo.h"
#include "pubstr.h"
#include "pubtime.h"
#include "simpletime.h"

// 任务日程
struct TaskSchedule
{
public:
	TaskSchedule(): seq_id(0), activate('\0')
	{}

public:
	// 是否已激活
	bool IsActivated() const
	{ return ('1' == activate); }

	// 是否为临时任务
	bool IsTemporaryTask() const
	{ return (base::PubStr::TrimUpperB(task_type) == "T"); }

	// 是否为常驻任务
	bool IsPermanentTask() const
	{ return (base::PubStr::TrimUpperB(task_type) == "P"); }

public:
	int         seq_id;					// 序号
	char        activate;				// 是否激活（有效）
	std::string task_type;				// 任务类型
	std::string kpi_id;					// 指标 ID
	std::string task_cycle;				// 任务周期
	std::string etl_time;				// 采集时间
	std::string task_state;				// 任务状态
	std::string task_state_desc;		// 任务状态描述
	std::string expiry_date_start;		// 有效期开始
	std::string expiry_date_end;		// 有效期结束
};

// 任务周期
class TaskCycle
{
public:
	TaskCycle()
	{ Clear(); }

	static const int ANY_TIME = -1;

public:
	// 是否有效
	bool IsValid() const
	{ return valid; }

	// 设置周期
	bool Set(const std::string& tc)
	{
		// 初始化
		Clear();

		std::vector<std::string> vec_str;
		base::PubStr::Str2StrVector(tc, "-", vec_str);

		if ( vec_str.size() == 6 )
		{
			int s_y  = 0;
			int s_m  = 0;
			int s_d  = 0;
			int s_h  = 0;
			int s_mi = 0;
			int s_s  = 0;

			int index = 0;
			std::string& ref_y = vec_str[index++];
			if ( "*" == ref_y )
			{
				s_y = ANY_TIME;
			}
			else if ( !base::PubStr::Str2Int(ref_y, s_y) || s_y <= 1970 )
			{
				return false;
			}

			std::string& ref_m = vec_str[index++];
			if ( "*" == ref_m )
			{
				s_m = ANY_TIME;
			}
			else if ( !base::PubStr::Str2Int(ref_m, s_m) || s_m < 1 || s_m > 12 )
			{
				return false;
			}

			std::string& ref_d = vec_str[index++];
			if ( "*" == ref_d )
			{
				s_d = ANY_TIME;
			}
			else if ( !base::PubStr::Str2Int(ref_d, s_d) || s_d < 1 || s_d > 31 )
			{
				return false;
			}

			std::string& ref_h = vec_str[index++];
			if ( "*" == ref_h )
			{
				s_h = ANY_TIME;
			}
			else if ( !base::PubStr::Str2Int(ref_h, s_h) || s_h < 0 || s_h > 23 )
			{
				return false;
			}

			std::string& ref_mi = vec_str[index++];
			if ( "*" == ref_mi )
			{
				s_mi = ANY_TIME;
			}
			else if ( !base::PubStr::Str2Int(ref_mi, s_mi) || s_mi < 0 || s_mi > 59 )
			{
				return false;
			}

			std::string& ref_s = vec_str[index++];
			if ( "*" == ref_s )
			{
				s_s = ANY_TIME;
			}
			else if ( !base::PubStr::Str2Int(ref_s, s_s) || s_s < 0 || s_s > 59 )
			{
				return false;
			}

			year = s_y;
			mon  = s_m;
			day  = s_d;
			hour = s_h;
			min  = s_mi;
			sec  = s_s;
			return (valid = true);
		}

		return false;
	}

	// 是否到时间点
	bool IsCycleTimeUp()
	{
		if ( valid )
		{
			base::SimpleTime st_now = base::SimpleTime::Now();

			if ( year != ANY_TIME && st_now.GetYear() != year )
			{
				return false;
			}
			if ( mon != ANY_TIME && st_now.GetMon() != mon )
			{
				return false;
			}
			if ( day != ANY_TIME && st_now.GetDay() != day )
			{
				return false;
			}
			if ( hour != ANY_TIME && st_now.GetHour() != hour )
			{
				return false;
			}
			if ( min != ANY_TIME && st_now.GetMin() != min )
			{
				return false;
			}
			if ( sec != ANY_TIME && st_now.GetSec() != sec )
			{
				return false;
			}

			return true;
		}

		return false;
	}

private:
	void Clear()
	{
		valid = false;
		year = 0;
		mon  = 0;
		day  = 0;
		hour = 0;
		min  = 0;
		sec  = 0;
	}

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
	EtlTime(): dt_type(base::PubTime::DT_UNKNOWN)
	{}

public:
	// 设置采集时间
	bool SetTime(const std::string& time)
	{
		// 初始化
		dt_type = base::PubTime::DT_UNKNOWN;
		std::vector<int>().swap(vecTime);

		std::vector<std::string> vec_str;
		if ( time.find(',') != std::string::npos )		// 格式一：[时间类型],[时间段]
		{
			base::PubStr::Str2StrVector(time, ",", vec_str);
			if ( vec_str.size() != 2 )
			{
				return false;
			}

			const std::string DTYPE = base::PubStr::TrimUpperB(vec_str[0]);
			if ( DTYPE == "DAY" )
			{
			}
			else if ( DTYPE == "MON" )
			{
			}
			else	// 不支持时间类型
			{
				return false;
			}
		}
		else	// 格式一：[时间类型]+/-[时间数]
		{
		}
	}

	// 是否有效
	bool IsValid() const
	{ return (dt_type != base::PubTime::DT_UNKNOWN); }


private:
	base::PubTime::DATE_TYPE dt_type;		// 时间类型
	std::vector<int>         vecTime;		// 时间列表
};

// 稽核任务
struct RATask
{
public:
	// 任务类型
	enum TASK_TYPE
	{
		TTYPE_U = 0,		// 未知任务（预设值）
		TTYPE_T = 1,		// 临时任务
		TTYPE_P = 2,		// 常驻任务
	};

	RATask(): seq_id(0), type(TTYPE_U)
	{}

public:
	int         seq_id;					// 序号
	TASK_TYPE   type;					// 任务类型
	std::string kpi_id;					// 指标 ID
	TaskCycle   cycle;					// 任务周期
	EtlTime     etl_time;				// 采集时间
	long long   exprry_date_start;		// 有效期开始
	long long   expiry_date_end;		// 有效期结束

	std::vector<
};

