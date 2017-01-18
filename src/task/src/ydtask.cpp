#include "ydtask.h"
#include "log.h"

YDTask::YDTask(base::Config& cfg)
:Task(cfg)
{
	// 日志文件前缀
	base::Log::SetLogFilePrefix("YDTask");
}

YDTask::~YDTask()
{
}

std::string YDTask::Version()
{
	return ("YDTask: "+Task::Version());
}

void YDTask::LoadConfig() throw(base::Exception)
{
}

void YDTask::Init() throw(base::Exception)
{
}

bool YDTask::ConfirmQuit()
{
	m_pLog->Output("Ready to confirm to quit ...... Sleep 10 seconds ...");
	sleep(10);
	return true;
}

void YDTask::GetNewTask() throw(base::Exception)
{
}

void YDTask::ShowTasksInfo()
{
}

void YDTask::HandleAnaTask() throw(base::Exception)
{
}

void YDTask::HandleEtlTask() throw(base::Exception)
{
}

void YDTask::BuildNewTask() throw(base::Exception)
{
}

void YDTask::FinishTask() throw(base::Exception)
{
}

