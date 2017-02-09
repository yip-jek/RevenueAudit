#include "ydtask.h"
#include <errno.h>
#include <string.h>
#include "config.h"
#include "log.h"
#include "ydtaskdb2.h"

YDTask::YDTask(base::Config& cfg)
:Task(cfg)
,m_pTaskDB2(NULL)
{
	// 日志文件前缀
	base::Log::SetLogFilePrefix("YDTask");
}

YDTask::~YDTask()
{
	ReleaseDB();
}

std::string YDTask::Version()
{
	return ("YDTask: "+Task::Version());
}

void YDTask::ReleaseDB()
{
	if ( m_pTaskDB2 != NULL )
	{
		delete m_pTaskDB2;
		m_pTaskDB2 = NULL;
	}
}

void YDTask::LoadConfig() throw(base::Exception)
{
	// 读取数据库连接配置
	m_pCfg->RegisterItem("DATABASE", "DB_NAME");
	m_pCfg->RegisterItem("DATABASE", "USER_NAME");
	m_pCfg->RegisterItem("DATABASE", "PASSWORD");

	// 读取采集与分析程序相关配置
	m_pCfg->RegisterItem("COMMON", "HIVE_AGENT_PATH");
	m_pCfg->RegisterItem("COMMON", "BIN_VER");
	m_pCfg->RegisterItem("COMMON", "ACQUIRE_BIN");
	m_pCfg->RegisterItem("COMMON", "ACQUIRE_MODE");
	m_pCfg->RegisterItem("COMMON", "ACQUIRE_CONFIG");
	m_pCfg->RegisterItem("COMMON", "ANALYSE_BIN");
	m_pCfg->RegisterItem("COMMON", "ANALYSE_MODE");
	m_pCfg->RegisterItem("COMMON", "ANALYSE_CONFIG");

	// 读取数据库表配置
	m_pCfg->RegisterItem("TABLE", "TAB_TASK_SCHE");
	m_pCfg->RegisterItem("TABLE", "TAB_TASK_SCHE_LOG");
	m_pCfg->RegisterItem("TABLE", "TAB_KPI_RULE");
	m_pCfg->RegisterItem("TABLE", "TAB_ETL_RULE");

	m_pCfg->ReadConfig();

	m_dbinfo.db_inst = m_pCfg->GetCfgValue("DATABASE", "DB_NAME");
	m_dbinfo.db_user = m_pCfg->GetCfgValue("DATABASE", "USER_NAME");
	m_dbinfo.db_pwd  = m_pCfg->GetCfgValue("DATABASE", "PASSWORD");

	m_hiveAgentPath = m_pCfg->GetCfgValue("COMMON", "HIVE_AGENT_PATH");
	m_binVer        = m_pCfg->GetCfgValue("COMMON", "BIN_VER");
	m_binAcquire    = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_BIN");
	m_modeAcquire   = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_MODE");
	m_cfgAcquire    = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_CONFIG");
	m_binAnalyse    = m_pCfg->GetCfgValue("COMMON", "ANALYSE_BIN");
	m_modeAnalyse   = m_pCfg->GetCfgValue("COMMON", "ANALYSE_MODE");
	m_cfgAnalyse    = m_pCfg->GetCfgValue("COMMON", "ANALYSE_CONFIG");

	m_tabTaskSche    = m_pCfg->GetCfgValue("TABLE", "TAB_TASK_SCHE");
	m_tabTaskScheLog = m_pCfg->GetCfgValue("TABLE", "TAB_TASK_SCHE_LOG");
	m_tabKpiRule     = m_pCfg->GetCfgValue("TABLE", "TAB_KPI_RULE");
	m_tabEtlRule     = m_pCfg->GetCfgValue("TABLE", "TAB_ETL_RULE");

	m_pLog->Output("[YD_TASK] Load config OK.");
}

void YDTask::InitConnect() throw(base::Exception)
{
	ReleaseDB();

	m_pTaskDB2 = new YDTaskDB2(m_dbinfo);
	if ( NULL == m_pTaskDB2 )
	{
		throw base::Exception(YDTERR_INIT_CONN, "Operator new YDTaskDB2 failed: 无法申请到内存空间！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pTaskDB2->SetTabTaskSche(m_tabTaskSche);
	m_pTaskDB2->SetTabTaskScheLog(m_tabTaskScheLog);
	m_pTaskDB2->SetTabKpiRule(m_tabKpiRule);
	m_pTaskDB2->SetTabEtlRule(m_tabEtlRule);
	m_pTaskDB2->Connect();
}

void YDTask::Init() throw(base::Exception)
{
	InitConnect();

	// 指定工作目录为Hive代理的路径
	if ( chdir(m_hiveAgentPath.c_str()) < 0 )
	{
		throw base::Exception(YDTERR_INIT, "Change work dir to '%s' failed! %s [FILE:%s, LINE:%d]", m_hiveAgentPath.c_str(), strerror(errno), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Change work dir to '%s' OK.", m_hiveAgentPath.c_str());

	if ( !m_pTaskDB2->IsTableExists(m_tabTaskSche) )
	{
		throw base::Exception(YDTERR_INIT, "The task schedule table is not existed: %s [FILE:%s, LINE:%d]", m_tabTaskSche.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Check the task schedule table [%s] OK.", m_tabTaskSche.c_str());

	if ( !m_pTaskDB2->IsTableExists(m_tabTaskScheLog) )
	{
		throw base::Exception(YDTERR_INIT, "The task schedule log table is not existed: %s [FILE:%s, LINE:%d]", m_tabTaskScheLog.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Check the task schedule log table [%s] OK.", m_tabTaskScheLog.c_str());

	if ( !m_pTaskDB2->IsTableExists(m_tabKpiRule) )
	{
		throw base::Exception(YDTERR_INIT, "The kpi rule table is not existed: %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Check the kpi rule table [%s] OK.", m_tabKpiRule.c_str());

	if ( !m_pTaskDB2->IsTableExists(m_tabEtlRule) )
	{
		throw base::Exception(YDTERR_INIT, "The etl rule table is not existed: %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Check the etl rule table [%s] OK.", m_tabEtlRule.c_str());

	m_pLog->Output("[YD_TASK] Init OK.");
}

bool YDTask::ConfirmQuit()
{
	return (m_mTaskSche.empty()
		&& m_mTaskSche_bak.empty()
		&& m_mTaskWait.empty()
		&& m_mEtlTaskRun.empty()
		&& m_mAnaTaskRun.empty()
		&& m_mTaskEnd.empty() );
}

void YDTask::GetNewTask() throw(base::Exception)
{
	m_pTaskDB2->GetTaskSchedule(m_mTaskSche);

	GetNewTaskSche();

	DelUnavailableTask();
}

void YDTask::GetNewTaskSche()
{
	// 获取任务日程：包括新增的和更新的
	int c_new_tasks = 0;
	int c_upd_tasks = 0;
	int c_err_tasks = 0;
	RATask rat;
	std::map<int, TaskSchedule>::iterator it;
	std::map<int, TaskSchedule>::iterator bk_it;
	for ( it = m_mTaskSche.begin(); it != m_mTaskSche.end(); ++it )
	{
		bk_it = m_mTaskSche_bak.find(it->first);
		if ( bk_it != m_mTaskSche_bak.end() )	// 已存在
		{
			if ( it->second != bk_it->second )	// 更新
			{
				if ( rat.LoadFromTaskSche(it->second) )
				{
					++c_upd_tasks;
					m_mTaskWait[it->first] = rat;
					m_mTaskSche_bak[it->first] = it->second;
					m_pLog->Output("[YD_TASK] 更新任务: ID=[%d], KPI_ID=[%s]", it->first, rat.kpi_id.c_str());
				}
				else	// 错误：载入任务日程失败，不更新
				{
					++c_err_tasks;
					m_pLog->Output("[YD_TASK] 任务日程载入失败，取消更新任务：ID=[%d]", it->first);
				}
			}
		}
		else	// 不存在
		{
			if ( rat.LoadFromTaskSche(it->second) )		// 新增
			{
				++c_new_tasks;
				m_mTaskWait[it->first] = rat;
				m_mTaskSche_bak[it->first] = it->second;
				m_pLog->Output("[YD_TASK] 新增任务: ID=[%d], KPI_ID=[%s]", it->first, rat.kpi_id.c_str());
			}
			else	// 错误：载入任务日程失败，不新增
			{
				++c_err_tasks;
				m_pLog->Output("[YD_TASK] 任务日程载入失败，取消新增任务：ID=[%d]", it->first);
			}
		}
	}

	m_pLog->Output("[YD_TASK] 新增任务数: %d", c_new_tasks);
	m_pLog->Output("[YD_TASK] 更新任务数: %d", c_upd_tasks);
	m_pLog->Output("[YD_TASK] 错误任务数: %d", c_err_tasks);
}

void YDTask::DelUnavailableTask()
{
	// 删除不存在或者没有激活的任务
	std::map<int, TaskSchedule>::iterator bk_it = m_mTaskSche_bak.begin();
	while ( bk_it != m_mTaskSche_bak.end() )
	{
		if ( m_mTaskSche.find(bk_it->first) == m_mTaskSche.end() )	// 不存在或者没有激活
		{
			if ( m_mTaskWait.find(bk_it->first) != m_mTaskWait.end() )
			{
				m_pLog->Output("[YD_TASK] 删除不存在或者没激活的任务：ID=[%d], KPI_ID=[%s]", bk_it->first, m_mTaskWait[bk_it->first].kpi_id.c_str());
				m_mTaskWait.erase(bk_it->first);
			}
			else
			{
				m_sDelAfterRun.insert(bk_it->first);
				m_pLog->Output("[YD_TASK] ：ID=[%d], KPI_ID=[%s]", bk_it->first, m_mTaskWait[bk_it->first].kpi_id.c_str());
			}

			m_mTaskSche_bak.erase(bk_it++);
		}
		else
		{
			++bk_it;
		}
	}
}

void YDTask::GetNoTask() throw(base::Exception)
{
	m_mTaskSche.clear();
	m_mTaskSche_bak.clear();
	m_mTaskWait.clear();
}

void YDTask::ShowTasksInfo()
{
	m_pLog->Output(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
	m_pLog->Output("[YD_TASK] 任务日程数: %llu", m_mTaskSche.size());
	m_pLog->Output("[YD_TASK] 日程备份数: %llu", m_mTaskSche_bak.size());
	m_pLog->Output("[YD_TASK] 任务等待数: %llu", m_mTaskWait.size());
	m_pLog->Output("[YD_TASK] 采集任务数: %llu", m_mEtlTaskRun.size());
	m_pLog->Output("[YD_TASK] 分析任务数: %llu", m_mAnaTaskRun.size());
	m_pLog->Output("[YD_TASK] 任务完成数: %llu", m_mTaskEnd.size());
	m_pLog->Output("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
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

