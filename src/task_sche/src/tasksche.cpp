#include "tasksche.h"
#include "config.h"
#include "log.h"
#include "taskdb2.h"


TaskSche::TaskSche(base::Config& cfg)
:m_pCfg(&cfg)
,m_pLog(base::Log::Instance())
,m_waitSeconds(0)
,m_pTaskDB(NULL)
{
}

TaskSche::~TaskSche()
{
	ReleaseDB();

	base::Log::Release();
}

const char* TaskSche::Version()
{
	return ("TaskSche: Version 1.0000 released. Compiled at "__TIME__" on "__DATE__);
}

void TaskSche::Do() throw(base::Exception)
{
	Init();

	DealTasks();
}

void TaskSche::ReleaseDB()
{
	if ( m_pTaskDB != NULL )
	{
		delete m_pTaskDB;
		m_pTaskDB = NULL;
	}
}

void TaskSche::Init() throw(base::Exception)
{
	LoadConfig();

	InitConnect();

	Check();
}

void TaskSche::LoadConfig() throw(base::Exception)
{
	// 读取任务配置
	m_pCfg->RegisterItem("SYS", "TIME_SECONDS");

	m_pCfg->RegisterItem("DATABASE", "DB_NAME");
	m_pCfg->RegisterItem("DATABASE", "USER_NAME");
	m_pCfg->RegisterItem("DATABASE", "PASSWORD");

	m_pCfg->RegisterItem("TABLE", "TAB_TASK_REQUEST");
	m_pCfg->RegisterItem("TABLE", "TAB_RA_KPI");

	m_pCfg->ReadConfig();

	m_waitSeconds = (long)m_pCfg->GetCfgLongVal("SYS", "TIME_SECONDS");

	m_dbInfo.db_inst = m_pCfg->GetCfgValue("DATABASE", "DB_NAME");
	m_dbInfo.db_user = m_pCfg->GetCfgValue("DATABASE", "USER_NAME");
	m_dbInfo.db_pwd  = m_pCfg->GetCfgValue("DATABASE", "PASSWORD");

	m_tabTaskReq = m_pCfg->GetCfgValue("TABLE", "TAB_TASK_REQUEST");
	m_tabRaKpi   = m_pCfg->GetCfgValue("TABLE", "TAB_RA_KPI");
}

void TaskSche::InitConnect() throw(base::Exception)
{
	ReleaseDB();

	m_pTaskDB = new TaskDB2(m_dbInfo);
	m_pTaskDB->SetTabTaskRequest(m_tabTaskReq);
	m_pTaskDB->SetTabRaKpi(m_tabRaKpi);
	m_pTaskDB->Connect();
}

void TaskSche::Check() throw(base::Exception)
{
	if ( m_waitSeconds <= 0 )
	{
		throw base::Exception(TERROR_CHECK, "Invalid time seconds: %ld [FILE:%s, LINE:%d]", m_waitSeconds, __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] Check time seconds OK. [Wait seconds: %ld]", m_waitSeconds);

	if ( !m_pTaskDB->IsTableExists(m_tabTaskReq) )
	{
		throw base::Exception(TERROR_CHECK, "The task request table is not existed: %s [FILE:%s, LINE:%d]", m_tabTaskReq.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] Check the task request table OK.");

	if ( !m_pTaskDB->IsTableExists(m_tabRaKpi) )
	{
		throw base::Exception(TERROR_CHECK, "The kpi table is not existed: %s [FILE:%s, LINE:%d]", m_tabRaKpi.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] Check the kpi table OK.");
}

void TaskSche::DealTasks() throw(base::Exception)
{
	int count = 0;
	while ( ++count < 5 )
	{
		m_pLog->Output("[TASK] Deal the task %d, sleep %ld second(s) ...", count, m_waitSeconds);
		sleep(m_waitSeconds);
	}
}

