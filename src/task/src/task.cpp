#include "task.h"
#include <errno.h>
#include "pubstr.h"
#include "simpletime.h"

Task::Task(base::Config& cfg)
:m_pCfg(&cfg)
,m_pLog(base::Log::Instance())
,m_TIDAccumulator(0)
,m_waitSeconds(0)
{
}

Task::~Task()
{
	base::Log::Release();
}

std::string Task::Version()
{
	return ("Version 2.0000 released. Compiled at "__TIME__" on "__DATE__);
}

void Task::Run() throw(base::Exception)
{
	Init();

	DealTasks();
}

void Task::DealTasks() throw(base::Exception)
{
	while ( true )
	{
		GetNewTask();

		ShowTask();

		ExecuteTask();

		FinishTask();
	}
}

void Task::ExecuteTask()
{
	HandleAnaTask();

	HandleEtlTask();

	BuildNewTask();
}

bool Task::IsProcessAlive(long long proc_task_id) throw(base::Exception)
{
	std::string str_cmd;
	base::PubStr::SetFormatString(str_cmd, "ps -ef | grep -w %lld | grep -v grep", proc_task_id);

	FILE* fp_pipe = popen(str_cmd.c_str(), "r");
	if ( NULL == fp_pipe )
	{
		throw base::Exception(TERROR_IS_PROC_EXIST, "Popen() failed: (%d) %s [FILE:%s, LINE:%d]", errno, strerror(errno), __FILE__, __LINE__);
	}

	char buffer[512] = "";
	fgets(buffer, 512, fp_pipe);

	pclose(fp_pipe);
	return (strlen(buffer) > 1);
}

long long Task::GenerateTaskID()
{
	// 取14位时间（格式：YYYYMMDDHHMISS）的后12位
	std::string time_str = base::SimpleTime::Now().Time14().substr(2);

	long long new_taskid = 0;
	base::PubStr::Str2LLong(time_str, new_taskid);

	// 加上累加值
	new_taskid *= 1000;
	new_taskid += (m_TIDAccumulator++);

	// 累加值复位
	if ( m_TIDAccumulator >= 1000 )
	{
		m_TIDAccumulator = 0;
	}

	return new_taskid;
}

std::string Task::EtlTimeTransform(const std::string& cycle) throw(base::Exception)
{
	if ( cycle.size() != 8U )
	{
		throw base::Exception(TERROR_ETLTIME_TRANSFORM, "The statistic cycle size is less than 8: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	int year = 0;
	int mon  = 0;
	int day  = 0;

	if ( !base::PubStr::Str2Int(cycle.substr(0, 4), year) || year < 1970 )
	{
		throw base::Exception(TERROR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::Str2Int(cycle.substr(4, 2), mon) || mon < 1 || mon > 12 )
	{
		throw base::Exception(TERROR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::Str2Int(cycle.substr(6, 2), day) || day < 1 || day > base::SimpleTime::LastDayOfTheMon(year, mon) )
	{
		throw base::Exception(TERROR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	long day_apart = base::PubTime::DayApartFromToday(year, mon, day);
	if ( day_apart < 0 )
	{
		std::string today = base::SimpleTime::Now().DayTime8();
		throw base::Exception(TERROR_ETLTIME_TRANSFORM, "The statistic cycle is greater than today (%s): %s [FILE:%s, LINE:%d]", today.c_str(), cycle.c_str(), __FILE__, __LINE__);
	}

	std::string etlrule_time;
	base::PubStr::SetFormatString(etlrule_time, "day-%ld", day_apart);
	return etlrule_time;
}

