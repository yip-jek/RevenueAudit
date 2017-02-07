#include "task.h"
#include <errno.h>
#include <string.h>
#include "log.h"
#include "config.h"
#include "gsignal.h"
#include "pubstr.h"
#include "pubtime.h"
#include "simpletime.h"

Task::Task(base::Config& cfg)
:m_pCfg(&cfg)
,m_pLog(base::Log::Instance())
,m_state(TS_BEGIN)
,m_TIDAccumulator(0)
,m_waitSeconds(0)
,m_showSeconds(0)
,m_showTimer(0)
{
}

Task::~Task()
{
	base::Log::Release();
}

std::string Task::Version()
{
	return ("Version 3.0005 released. Compiled at "__TIME__" on "__DATE__);
}

void Task::Run() throw(base::Exception)
{
	InitBaseConfig();

	LoadConfig();

	Init();

	DealTasks();
}

void Task::InitBaseConfig() throw(base::Exception)
{
	m_pCfg->RegisterItem("SYS", "TIME_SECONDS");
	m_pCfg->RegisterItem("SYS", "TASK_SHOW_SECONDS");
	m_pCfg->ReadConfig();

	m_waitSeconds = m_pCfg->GetCfgLongVal("SYS", "TIME_SECONDS");
	if ( m_waitSeconds <= 0 )
	{
		throw base::Exception(TERR_INIT_BASE_CFG, "Invalid wait time seconds: %ld [FILE:%s, LINE:%d]", m_waitSeconds, __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] Check wait time seconds OK. [TIME SECONDS: %ld]", m_waitSeconds);

	m_showSeconds = m_pCfg->GetCfgLongVal("SYS", "TASK_SHOW_SECONDS");
	if ( m_showSeconds <= 0 )
	{
		throw base::Exception(TERR_INIT_BASE_CFG, "Invalid task show seconds: %ld [FILE:%s, LINE:%d]", m_showSeconds, __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] Check task show seconds OK. [SHOW SECONDS: %ld]", m_showSeconds);
}

bool Task::Running()
{
	switch ( m_state )
	{
	case TS_BEGIN:			// 状态：开始
		// 改变状态为运行中
		m_state = TS_RUNNING;
		break;
	case TS_RUNNING:		// 状态：运行中
		// 收到退出信号？
		if ( !GSignal::IsRunning() )
		{
			m_state = TS_END;
		}
		break;
	case TS_END:			// 状态：结束（收到退出信号）
		// 确认退出？
		if ( ConfirmQuit() )
		{
			m_state = TS_QUIT;
		}
		break;
	case TS_QUIT:			// 状态：退出
	default:				// 状态：未知
		// Do nothing !
		break;
	}

	return (TS_QUIT != m_state);
}

void Task::DealTasks() throw(base::Exception)
{
	// 设置退出信号捕获
	if ( !GSignal::Init(m_pLog) )
	{
		throw base::Exception(TERR_DEAL_TASKS, "Init setting signal failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 设置计时
	SecTimer wait_timer(m_waitSeconds);
	m_showTimer.Set(m_showSeconds);
	wait_timer.Start();
	m_showTimer.Start();

	while ( Running() )
	{
		GetNewTask();

		ShowTask();

		ExecuteTask();

		FinishTask();

		// 等待下一次的任务执行
		wait_timer.WaitForTimeUp();
	}
}

void Task::GetTasks() throw(base::Exception)
{
	// 是否准备退出？
	if ( m_state != TS_END )
	{
		GetNewTask();
	}
	else	// 不获取新任务
	{
		GetNoTask();
	}
}

void Task::ShowTask()
{
	if ( m_showTimer.IsTimeUp() )
	{
		ShowTasksInfo();
	}
}

void Task::ExecuteTask() throw(base::Exception)
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
		throw base::Exception(TERR_IS_PROC_EXIST, "Popen() failed: (%d) %s [FILE:%s, LINE:%d]", errno, strerror(errno), __FILE__, __LINE__);
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
		throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle size is less than 8: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	int year = 0;
	int mon  = 0;
	int day  = 0;

	if ( !base::PubStr::Str2Int(cycle.substr(0, 4), year) || year < 1970 )
	{
		throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::Str2Int(cycle.substr(4, 2), mon) || mon < 1 || mon > 12 )
	{
		throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::Str2Int(cycle.substr(6, 2), day) || day < 1 || day > base::SimpleTime::LastDayOfTheMon(year, mon) )
	{
		throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	long day_apart = base::PubTime::DayApartFromToday(year, mon, day);
	if ( day_apart < 0 )
	{
		std::string today = base::SimpleTime::Now().DayTime8();
		throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle is greater than today (%s): %s [FILE:%s, LINE:%d]", today.c_str(), cycle.c_str(), __FILE__, __LINE__);
	}

	std::string etlrule_time;
	base::PubStr::SetFormatString(etlrule_time, "day-%ld", day_apart);
	return etlrule_time;
}

