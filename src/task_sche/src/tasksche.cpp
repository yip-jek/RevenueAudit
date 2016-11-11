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

	m_pCfg->RegisterItem("COMMON", "HIVE_AGENT_PATH");
	m_pCfg->RegisterItem("COMMON", "ACQUIRE_BIN");
	m_pCfg->RegisterItem("COMMON", "ACQUIRE_CONFIG");
	m_pCfg->RegisterItem("COMMON", "ANALYSE_BIN");
	m_pCfg->RegisterItem("COMMON", "ANALYSE_CONFIG");

	m_pCfg->RegisterItem("TABLE", "TAB_TASK_REQUEST");
	m_pCfg->RegisterItem("TABLE", "TAB_KPI_RULE");
	m_pCfg->RegisterItem("TABLE", "TAB_ETL_RULE");

	m_pCfg->ReadConfig();

	m_waitSeconds = (long)m_pCfg->GetCfgLongVal("SYS", "TIME_SECONDS");

	m_dbInfo.db_inst = m_pCfg->GetCfgValue("DATABASE", "DB_NAME");
	m_dbInfo.db_user = m_pCfg->GetCfgValue("DATABASE", "USER_NAME");
	m_dbInfo.db_pwd  = m_pCfg->GetCfgValue("DATABASE", "PASSWORD");

	m_hiveAgentPath = m_pCfg->GetCfgValue("COMMON", "HIVE_AGENT_PATH");
	m_binAcquire    = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_BIN");
	m_cfgAcquire    = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_CONFIG");
	m_binAnalyse    = m_pCfg->GetCfgValue("COMMON", "ANALYSE_BIN");
	m_cfgAnalyse    = m_pCfg->GetCfgValue("COMMON", "ANALYSE_CONFIG");

	m_tabTaskReq = m_pCfg->GetCfgValue("TABLE", "TAB_TASK_REQUEST");
	m_tabKpiRule = m_pCfg->GetCfgValue("TABLE", "TAB_KPI_RULE");
	m_tabEtlRule = m_pCfg->GetCfgValue("TABLE", "TAB_ETL_RULE");
}

void TaskSche::InitConnect() throw(base::Exception)
{
	ReleaseDB();

	m_pTaskDB = new TaskDB2(m_dbInfo);
	m_pTaskDB->SetTabTaskRequest(m_tabTaskReq);
	m_pTaskDB->SetTabKpiRule(m_tabKpiRule);
	m_pTaskDB->SetTabEtlRule(m_tabEtlRule);
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

	if ( !m_pTaskDB->IsTableExists(m_tabKpiRule) )
	{
		throw base::Exception(TERROR_CHECK, "The kpi rule table is not existed: %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] Check the kpi rule table OK.");

	if ( !m_pTaskDB->IsTableExists(m_tabEtlRule) )
	{
		throw base::Exception(TERROR_CHECK, "The etl rule table is not existed: %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] Check the etl rule table OK.");
}

void TaskSche::DealTasks() throw(base::Exception)
{
	while ( true )
	{
		GetNewTask();

		CheckTask();

		ExecuteTask();

		FinishTask();
	}
}

void TaskSche::GetNewTask()
{
	m_pTaskDB->SelectNewTaskRequest(m_vecNewTask);

	if ( m_vecNewTask.empty() )
	{
		m_pLog->Output("[TASK] No new task found.");
	}
	else
	{
		m_pLog->Output("[TASK] Get the new task size: %lu", m_vecNewTask.size());
	}

	const int VEC_TRI_SIZE = vecTaskReqInfo.size();
	for ( int i = 0; i < VEC_TRI_SIZE; ++i )
	{
		TaskReqInfo& ref_tri = vecTaskReqInfo[i];
		m_pLog->Output("[TASK] Get task %d: SEQ=[%d], KPI=[%s], CYCLE=[%s]", (i+1), ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str());

		if ( m_mTaskReqInfo.find(ref_tri.seq_id) != m_mTaskReqInfo.end() )
		{
			m_pLog->Output("<WARNING> [TASK] The task (SEQ=%d) already exists ! So drop it !", ref_tri.seq_id);
		}
		else
		{
			m_mTaskReqInfo[ref_tri.seq_id] = ref_tri;
		}
	}
}

void TaskSche::CheckTask()
{
}

void TaskSche::ExecuteTask()
{
}

void TaskSche::FinishTask()
{
	const int VEC_END_TASK_SIZE = m_vecEndTask.size();
	for ( int i = 0; i < VEC_END_TASK_SIZE; ++i )
	{
		TaskReqInfo& ref_tri = m_vecEndTask[i];

		m_pTaskDB->UpdateTaskRequest(ref_tri);
		m_pLog->Output("[TASK] Finish task: SEQ=[%d], KPI=[%s], CYCLE=[%s]", ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str());
	}

	// 清空已完成任务列表
	std::vector<TaskReqInfo>().swap(m_vecEndTask);
}

