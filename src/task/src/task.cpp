#include "task.h"
#include <errno.h>
#include <string.h>
#include "log.h"
#include "basedir.h"
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
,m_taskContinue(true)
{
}

Task::~Task()
{
	base::Log::Release();
}

std::string Task::Version()
{
	return ("Version 4.0.2.0 released. Compiled at " __TIME__ " on " __DATE__);
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
	m_pCfg->RegisterItem("COMMON", "TEMP_PATH");
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

	// 任务临时目录
	// 检查临时目录是否有效
	// 若临时目录不存在，则自动创建
	std::string temp_path = m_pCfg->GetCfgValue("COMMON", "TEMP_PATH");
	if ( !base::BaseDir::IsDirExist(temp_path) && !base::BaseDir::CreateFullPath(temp_path) )
	{
		throw base::Exception(TERR_INIT_BASE_CFG, "Create temp path '%s' failed: %s [FILE:%s, LINE:%d]", temp_path.c_str(), strerror(errno), __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] Check temp path OK: [%s]", temp_path.c_str());

	if ( !m_tsSwitch.Init(temp_path) )
	{
		throw base::Exception(TERR_INIT_BASE_CFG, "TaskStatusSwitch initialization failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] TaskStatusSwitch initialization OK.");
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
		if ( !base::GSignal::IsRunning() )
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
	if ( !base::GSignal::Init(m_pLog) )
	{
		throw base::Exception(TERR_DEAL_TASKS, "Init setting signal failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 设置计时
	SecTimer wait_timer(m_waitSeconds);
	m_showTimer.Set(m_showSeconds);
	wait_timer.Start();
	m_showTimer.Start();

    //异常退出后重置任务
    GetUndoneTask();

	while ( Running() )
	{
		CheckTaskStatus();

		GetTasks();

		ShowTask();

		ExecuteTask();

		FinishTask();

		// 等待下一次的任务执行
		wait_timer.WaitForTimeUp();
	}
}

void Task::CheckTaskStatus()
{
	bool pause  = m_tsSwitch.GetPauseState();
	bool frozen = m_tsSwitch.GetFrozenState();

	// 检查任务状态
	m_tsSwitch.Check();

	if ( pause != m_tsSwitch.GetPauseState() )
	{
		pause = !pause;
		if ( pause )
		{
			m_pLog->Output("[TASK] GET TASK STATE: [PAUSE] (暂停)");
		}
		else
		{
			m_pLog->Output("[TASK] GET TASK STATE: [CONTINUE] (继续)");
		}
	}

	if ( frozen != m_tsSwitch.GetFrozenState() )
	{
		frozen = !frozen;
		if ( frozen )
		{
			m_pLog->Output("[TASK] GET TASK STATE: [FROZEN] (冻结)");
		}
		else
		{
			m_pLog->Output("[TASK] GET TASK STATE: [THAWED] (解冻)");
		}
	}

	// 暂停或冻结期内，不获取也不下发新任务！
	m_taskContinue = (!pause && !frozen);
}

void Task::GetTasks() throw(base::Exception)
{
	// 是否准备退出？
	if ( m_state != TS_END )
	{
		// 暂停或冻结期内，不获取新任务！
		if ( m_taskContinue )
		{
			GetNewTask();
		}
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

	// 暂停或冻结期内，不下发新任务！
	if ( m_taskContinue )
	{
		BuildNewTask();
	}
}

std::string Task::FetchPipeBuffer(const std::string& cmd) throw(base::Exception)
{
	FILE* fp_pipe = popen(cmd.c_str(), "r");
	if ( NULL == fp_pipe )
	{
		throw base::Exception(TERR_IS_PROC_EXIST, "Popen(r:\"%s\") failed: (%d) %s [FILE:%s, LINE:%d]", cmd.c_str(), errno, strerror(errno), __FILE__, __LINE__);
	}

	char buffer[512] = "";
	fgets(buffer, 512, fp_pipe);

	pclose(fp_pipe);
	return buffer;
}

bool Task::IsProcessAlive(long long proc_task_id) throw(base::Exception)
{
	std::string str_cmd;
	base::PubStr::SetFormatString(str_cmd, "ps -ef | grep -w %lld | grep -v grep", proc_task_id);

	return !FetchPipeBuffer(str_cmd).empty();
}

bool Task::IsProcessAlive(const TaskInfo& task_info, const TaskReqInfo& tri, bool IsGDAudiKPIType) throw(base::Exception)
{
    //keyword e.g. :00001:KPI_YC_BOSSYHQFDZXQBHFF_YW:ANA_YC_BOSSYHQFDZXQBHFF_YW:21219:
	std::string str_cmd;
	if ( IsGDAudiKPIType )
	{
		base::PubStr::SetFormatString(str_cmd, "ps -ef | grep -w \"[a-zA-Z][a-zA-Z]-%s:%s:.*\" | grep -v grep | wc -l", tri.stat_cycle.c_str(), task_info.kpi_id.c_str());
	}
	else
	{
		base::PubStr::SetFormatString(str_cmd, "ps -ef | grep -w \"%s-%s:%s:.*\" | grep -v grep | wc -l", tri.stat_city.c_str(), tri.stat_cycle.c_str(), task_info.kpi_id.c_str());
	}

	int proc_count = 0;
	base::PubStr::Str2Int(FetchPipeBuffer(str_cmd), proc_count);
	return (proc_count > 0);
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
	if ( !base::PubStr::Str2Int(cycle.substr(0, 4), year) )
	{
		throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	int mon  = 0;
	if ( !base::PubStr::Str2Int(cycle.substr(4, 2), mon) )
	{
		throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	int day  = 0;
	if ( !base::PubStr::Str2Int(cycle.substr(6, 2), day) )
	{
		throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	// 时间无效
	const base::SimpleTime ST_CYCLE(year, mon, day, 0, 0, 0);
	if ( !ST_CYCLE.IsValid() )
	{
		throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle is invalid: %s [FILE:%s, LINE:%d]", cycle.c_str(), __FILE__, __LINE__);
	}

	const base::SimpleTime ST_NOW(base::SimpleTime::Now());
	long day_diff = base::PubTime::DayDifference(ST_CYCLE, ST_NOW);

	std::string etlrule_time;
	if ( day_diff < 0 )
	{
		//std::string today = ST_NOW.DayTime8();
		//throw base::Exception(TERR_ETLTIME_TRANSFORM, "The statistic cycle is greater than today (%s): %s [FILE:%s, LINE:%d]", today.c_str(), cycle.c_str(), __FILE__, __LINE__);
		base::PubStr::SetFormatString(etlrule_time, "day+%ld", -day_diff);
	}
	else
	{
		base::PubStr::SetFormatString(etlrule_time, "day-%ld", day_diff);
	}

	return etlrule_time;
}

void Task::GetUndoneTask() throw(base::Exception)
{
    //NOTHING
    return ;
}

