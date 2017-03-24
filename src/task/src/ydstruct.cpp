#include "ydstruct.h"


bool operator == (const TaskSchedule& ts1, const TaskSchedule& ts2)
{
	return (ts1.seq_id == ts2.seq_id
		&& base::PubStr::TrimUpperB(ts1.task_type) == base::PubStr::TrimUpperB(ts2.task_type)
		&& base::PubStr::TrimUpperB(ts1.kpi_id) == base::PubStr::TrimUpperB(ts2.kpi_id)
		&& base::PubStr::TrimB(ts1.task_cycle) == base::PubStr::TrimB(ts2.task_cycle)
		&& base::PubStr::TrimUpperB(ts1.etl_time) == base::PubStr::TrimUpperB(ts2.etl_time)
		&& base::PubStr::TrimB(ts1.expiry_date_start) == base::PubStr::TrimB(ts2.expiry_date_start)
		&& base::PubStr::TrimB(ts1.expiry_date_end) == base::PubStr::TrimB(ts2.expiry_date_end) );
}

bool operator != (const TaskSchedule& ts1, const TaskSchedule& ts2)
{
	return !(ts1 == ts2);
}


TaskSchedule::TaskSchedule()
:seq_id(0)
{
}

bool TaskSchedule::IsTemporaryTask() const
{
	return (base::PubStr::TrimUpperB(task_type) == "T");
}

bool TaskSchedule::IsPermanentTask() const
{
	return (base::PubStr::TrimUpperB(task_type) == "P");
}


////////////////////////////////////////////////////////////////////////////////
TaskCycle::TaskCycle()
{
	Clear();
}

bool TaskCycle::IsValid() const
{
	return valid;
}

bool TaskCycle::Set(const std::string& tc)
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
		else if ( !base::PubStr::Str2Int(ref_y, s_y) || s_y <= 1970 || s_y > 9999 )
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

		const int MAX_DAY = (s_y != ANY_TIME && s_m != ANY_TIME) ? base::SimpleTime::LastDayOfTheMon(s_y, s_m) : 31;
		std::string& ref_d = vec_str[index++];
		if ( "*" == ref_d )
		{
			s_d = ANY_TIME;
		}
		else if ( !base::PubStr::Str2Int(ref_d, s_d) || s_d < 1 || s_d > MAX_DAY )
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

bool TaskCycle::IsCycleTimeUp()
{
	if ( valid )
	{
		const base::SimpleTime ST_NOW = base::SimpleTime::Now();

		if ( year != ANY_TIME && ST_NOW.GetYear() != year )
		{
			return false;
		}
		if ( mon != ANY_TIME && ST_NOW.GetMon() != mon )
		{
			return false;
		}
		if ( day != ANY_TIME && ST_NOW.GetDay() != day )
		{
			return false;
		}
		if ( hour != ANY_TIME && ST_NOW.GetHour() != hour )
		{
			return false;
		}
		if ( min != ANY_TIME && ST_NOW.GetMin() != min )
		{
			return false;
		}
		if ( sec != ANY_TIME && ST_NOW.GetSec() != sec )
		{
			return false;
		}

		return true;
	}

	return false;
}

void TaskCycle::Clear()
{
	valid = false;
	year = 0;
	mon  = 0;
	day  = 0;
	hour = 0;
	min  = 0;
	sec  = 0;
}


////////////////////////////////////////////////////////////////////////////////
EtlTime::EtlTime()
:dt_type(base::PubTime::DT_UNKNOWN)
,curr_index(INVALID_INDEX)
{
}

bool EtlTime::SetTime(const std::string& time)
{
	// 初始化
	dt_type = base::PubTime::DT_UNKNOWN;
	std::vector<int>().swap(vec_time);
	etl_time.clear();
	curr_index = INVALID_INDEX;

	const std::string STR_DELIM = "=";
	base::PubTime::DATE_TYPE dt = base::PubTime::DT_UNKNOWN;
	if ( time.find(STR_DELIM) != std::string::npos )		// 格式一：[时间类型]=[时间段]
	{
		std::vector<std::string> vec_str;
		base::PubStr::Str2StrVector(time, STR_DELIM, vec_str);
		if ( vec_str.size() != 2 )
		{
			return false;
		}

		const std::string DTYPE = base::PubStr::TrimUpperB(vec_str[0]);
		if ( DTYPE == "DAY" )	// 日
		{
			dt = base::PubTime::DT_DAY;
		}
		else if ( DTYPE == "MON" )	// 月
		{
			dt = base::PubTime::DT_MONTH;
		}
		else	// 不支持时间类型
		{
			return false;
		}

		if ( !base::PubTime::SpreadTimeInterval(dt, vec_str[1], "-", vec_time) )
		{
			return false;
		}
	}
	else	// 格式二：[时间类型]+/-[时间数]
	{
		std::string str_date;
		if ( !base::PubTime::DateApartFromNow(time, dt, str_date) )
		{
			return false;
		}

		etl_time = time;
	}

	dt_type = dt;
	return true;
}

bool EtlTime::HaveNext() const
{
	return (curr_index != INVALID_INDEX);
}

bool EtlTime::IsValid() const
{
	return (dt_type != base::PubTime::DT_UNKNOWN);
}

void EtlTime::Init()
{
	curr_index = IsValid() ? 0 : INVALID_INDEX;
}

void EtlTime::GetNext(std::string& next_etl_time)
{
	if ( curr_index != INVALID_INDEX )
	{
		if ( etl_time.empty() )
		{
			next_etl_time = base::PubStr::Int2Str(vec_time[curr_index++]);

			if ( curr_index >= (int)vec_time.size() )
			{
				curr_index = INVALID_INDEX;
			}
		}
		else
		{
			curr_index = INVALID_INDEX;
			base::PubTime::DateApartFromNow(etl_time, dt_type, next_etl_time);
		}
	}
}

std::string EtlTime::Convert(const std::string& time)
{
	int date = 0;
	const std::string TIME = base::PubStr::TrimB(time);
	if ( TIME.empty() || !base::PubStr::Str2Int(TIME, date) )
	{
		return std::string();
	}

	int year = 0;
	int mon  = 0;
	int day  = 0;
	std::string str_diff_time;
	const base::SimpleTime ST_NOW = base::SimpleTime::Now();

	if ( date > 1000000 )		// YYYYMMDD
	{
		year = date / 10000;
		mon  = (date % 10000) / 100;
		day  = date % 100;

		long day_diff = base::PubTime::DayDifference(base::SimpleTime(year, mon, day, 0, 0, 0), ST_NOW);
		if ( day_diff >= 0 )
		{
			base::PubStr::SetFormatString(str_diff_time, "day-%ld", day_diff);
		}
		else
		{
			base::PubStr::SetFormatString(str_diff_time, "day+%ld", (-day_diff));
		}
	}
	else	// YYYYMM
	{
		year = date / 100;
		mon  = date % 100;

		int mon_diff = (ST_NOW.GetYear() - year) * 12 + ST_NOW.GetMon() - mon;
		if ( mon_diff >= 0 )
		{
			base::PubStr::SetFormatString(str_diff_time, "mon-%d", mon_diff);
		}
		else
		{
			base::PubStr::SetFormatString(str_diff_time, "mon+%d", (-mon_diff));
		}
	}

	return str_diff_time;
}


////////////////////////////////////////////////////////////////////////////////
const char* const TaskScheLog::S_APP_TYPE_ETL = "ETL";			// 采集程序类型
const char* const TaskScheLog::S_APP_TYPE_ANA = "ANA";			// 分析程序类型

TaskScheLog::TaskScheLog()
:log_id(0)
{
}

void TaskScheLog::Clear()
{
	log_id = 0;
	kpi_id.clear();
	sub_id.clear();
	task_id.clear();
	task_type.clear();
	etl_time.clear();
	app_type.clear();
	start_time.clear();
	end_time.clear();
	task_state.clear();
	state_desc.clear();
	remarks.clear();
}


////////////////////////////////////////////////////////////////////////////////
RATask::RATask()
:seq_id(0)
,type(TTYPE_U)
{
}

bool RATask::LoadFromTaskSche(const TaskSchedule& ts)
{
	RATask tmp_rat;

	if ( ts.seq_id < 0 )	// 无效的序号
	{
		return false;
	}
	tmp_rat.seq_id = ts.seq_id;
	tmp_rat.kpi_id = base::PubStr::TrimUpperB(ts.kpi_id);

	if ( ts.IsPermanentTask() )		// 常驻任务
	{
		tmp_rat.type = TTYPE_P;
	}
	else if ( ts.IsTemporaryTask() )	// 临时任务
	{
		tmp_rat.type = TTYPE_T;
	}
	else	// 未知
	{
		return false;
	}

	// 周期有效？
	if ( !tmp_rat.cycle.Set(ts.task_cycle) )
	{
		return false;
	}

	// 采集时间有效？
	if ( !tmp_rat.et_etl_time.SetTime(ts.etl_time) )
	{
		return false;
	}

	// 有效期开始时间有效？
	long long ll_time = 0;
	if ( !base::PubStr::Str2LLong(base::PubStr::TrimB(ts.expiry_date_start), ll_time) || !tmp_rat.st_expiry_start.Set(ll_time) )
	{
		return false;
	}

	// 有效期结束时间有效？
	if ( !base::PubStr::Str2LLong(base::PubStr::TrimB(ts.expiry_date_end), ll_time) || !tmp_rat.st_expiry_end.Set(ll_time) )
	{
		return false;
	}

	// 有效期的结束时间比开始时间还早？
	if ( tmp_rat.st_expiry_start > tmp_rat.st_expiry_end )
	{
		return false;
	}

	*this = tmp_rat;
	return true;
}

