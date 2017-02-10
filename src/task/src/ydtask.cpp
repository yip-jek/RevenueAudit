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
	m_pCfg->RegisterItem("COMMON", "ETL_STATE_SUCCESS");

	// 读取数据库表配置
	m_pCfg->RegisterItem("TABLE", "TAB_TASK_SCHE");
	m_pCfg->RegisterItem("TABLE", "TAB_TASK_SCHE_LOG");
	m_pCfg->RegisterItem("TABLE", "TAB_KPI_RULE");
	m_pCfg->RegisterItem("TABLE", "TAB_ETL_RULE");

	m_pCfg->ReadConfig();

	m_dbinfo.db_inst = m_pCfg->GetCfgValue("DATABASE", "DB_NAME");
	m_dbinfo.db_user = m_pCfg->GetCfgValue("DATABASE", "USER_NAME");
	m_dbinfo.db_pwd  = m_pCfg->GetCfgValue("DATABASE", "PASSWORD");

	m_hiveAgentPath   = m_pCfg->GetCfgValue("COMMON", "HIVE_AGENT_PATH");
	m_binVer          = m_pCfg->GetCfgValue("COMMON", "BIN_VER");
	m_binAcquire      = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_BIN");
	m_modeAcquire     = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_MODE");
	m_cfgAcquire      = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_CONFIG");
	m_binAnalyse      = m_pCfg->GetCfgValue("COMMON", "ANALYSE_BIN");
	m_modeAnalyse     = m_pCfg->GetCfgValue("COMMON", "ANALYSE_MODE");
	m_cfgAnalyse      = m_pCfg->GetCfgValue("COMMON", "ANALYSE_CONFIG");
	m_etlStateSuccess = m_pCfg->GetCfgValue("COMMON", "ETL_STATE_SUCCESS");

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
	int c_new_tasks = 0;
	int c_upd_tasks = 0;
	int c_err_tasks = 0;

	// 获取任务日程：包括新增的和更新的
	RATask rat;
	std::map<int, TaskSchedule>::iterator it;
	std::map<int, TaskSchedule>::iterator bk_it;
	for ( it = m_mTaskSche.begin(); it != m_mTaskSche.end(); ++it )
	{
		int id = it->first;
		bk_it = m_mTaskSche_bak.find(id);
		if ( bk_it != m_mTaskSche_bak.end() )	// 已存在
		{
			if ( it->second != bk_it->second )	// 更新
			{
				if ( rat.LoadFromTaskSche(it->second) )
				{
					++c_upd_tasks;
					m_mTaskWait[id] = rat;
					m_mTaskSche_bak[id] = it->second;
					m_pLog->Output("[YD_TASK] 更新任务: ID=[%d], KPI_ID=[%s]", id, rat.kpi_id.c_str());
				}
				else	// 错误：载入任务日程失败，不更新
				{
					++c_err_tasks;
					m_pLog->Output("[YD_TASK] 任务日程载入失败，取消更新任务：ID=[%d]", id);
				}
			}
		}
		else	// 不存在
		{
			if ( rat.LoadFromTaskSche(it->second) )		// 新增
			{
				++c_new_tasks;
				m_mTaskWait[id] = rat;
				m_mTaskSche_bak[id] = it->second;
				m_pLog->Output("[YD_TASK] 新增任务: ID=[%d], KPI_ID=[%s]", id, rat.kpi_id.c_str());
			}
			else	// 错误：载入任务日程失败，不新增
			{
				++c_err_tasks;
				m_pLog->Output("[YD_TASK] 任务日程载入失败，取消新增任务：ID=[%d]", id);
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
	int c_del_tasks = 0;
	std::map<int, TaskSchedule>::iterator bk_it = m_mTaskSche_bak.begin();
	while ( bk_it != m_mTaskSche_bak.end() )
	{
		int bk_id = bk_it->first;
		if ( m_mTaskSche.find(bk_id) == m_mTaskSche.end() )	// 不存在或者没有激活
		{
			if ( m_mTaskWait.find(bk_id) != m_mTaskWait.end() )
			{
				m_pLog->Output("[YD_TASK] 删除不存在或者没激活的任务：ID=[%d], KPI_ID=[%s]", bk_id, m_mTaskWait[bk_id].kpi_id.c_str());
				m_mTaskWait.erase(bk_id);
			}

			if ( m_mEtlTaskRun.find(bk_id) != m_mEtlTaskRun.end()
				|| m_mAnaTaskRun.find(bk_id) != m_mAnaTaskRun.end()
				|| m_mTaskEnd.find(bk_id) != m_mTaskEnd.end() )
			{
				m_sDelAfterRun.insert(bk_id);
				m_pLog->Output("[YD_TASK] 任务正在运行，运行结束后再删除：ID=[%d]", bk_id);
			}

			++c_del_tasks;
			m_mTaskSche_bak.erase(bk_it++);
		}
		else
		{
			++bk_it;
		}
	}

	m_pLog->Output("[YD_TASK] 删除任务数: %d", c_del_tasks);
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
	if ( m_mAnaTaskRun.empty() )
	{
		return;
	}

	// 获取分析任务状态
	std::map<int, RATask>::iterator it = m_mAnaTaskRun.begin();
	while ( it != m_mAnaTaskRun.end() )
	{
		RATask& ref_rat = it->second;
		TaskScheLog& ref_tslog = ref_rat.tslogAnaTask;
		m_pTaskDB2->SelectTaskScheLogState(ref_tslog);

		// 分析任务是否完成？
		if ( !ref_tslog.end_time.empty() && !ref_tslog.task_state.empty() )
		{
			m_pLog->Output("[YD_TASK] Analyse finished: SEQ=[%d], TASK_ID=[%s], KPI_ID=[%s], ANA_ID=[%s], ETL_TIME=[%s], END_TIME=[%s], TASK_STATE=[%s], STATE_DESC=[%s], REMARK=[%s]", ref_rat.seq_id, ref_tslog.task_id.c_str(), ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.etl_time.c_str(), ref_tslog.end_time.c_str(), ref_tslog.task_state.c_str(), ref_tslog.state_desc.c_str(), ref_tslog.remarks.c_str());

			m_mTaskEnd[ref_rat.seq_id] = ref_rat;
			m_mAnaTaskRun.erase(it++);
			continue;
		}
		else	// 未完成
		{
			long long ll_taskid = 0;
			if ( !base::PubStr::Str2LLong(ref_tslog.task_id, ll_taskid) )
			{
				throw base::Exception(YDTERR_HDL_ANA_TASK, "Convert the task_id (string) to long long failed: SEQ=[%d], KPI_ID=[%s], ANA_ID=[%s], TASK_ID=[%s] [FILE:%s, LINE:%d]", ref_rat.seq_id, ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.task_id.c_str(), __FILE__, __LINE__);
			}

			// 检查分析进程是否异常退出
			if ( !IsProcessAlive(ll_taskid) )		// 进程异常退出
			{
				m_pLog->Output("[YD_TASK] Analyse exited unexpectedly: SEQ=[%d], TASK_ID=[%lld], KPI_ID=[%s], ANA_ID=[%s], ETL_TIME=[%s]", ref_rat.seq_id, ll_taskid, ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.etl_time.c_str());

				ref_tslog.end_time   = base::SimpleTime::Now().Time14();
				ref_tslog.task_state = "ANALYSE_ABORT";
				ref_tslog.state_desc = "分析进程异常退出";
				m_pTaskDB2->UpdateTaskScheLogState(ref_tslog);

				m_mTaskEnd[ref_rat.seq_id] = ref_rat;
				m_mAnaTaskRun.erase(it++);
				continue;
			}
		}

		++it;
	}
}

void YDTask::HandleEtlTask() throw(base::Exception)
{
	if ( m_mEtlTaskRun.empty() )
	{
		return;
	}

	// 获取分析任务状态
	std::map<int, RATask>::iterator it = m_mEtlTaskRun.begin();
	while ( it != m_mEtlTaskRun.end() )
	{
		RATask& ref_rat = it->second;

		int c_etl_success  = 0;
		int c_etl_finished = 0;

		const int VEC_SIZE = ref_rat.vecEtlTasks.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			TaskScheLog& ref_tslog = ref_rat.vecEtlTasks[i];
			if ( !ref_tslog.end_time.empty() && !ref_tslog.task_state.empty() )	// 已获取状态
			{
				++c_etl_finished;
				if ( base::PubStr::TrimB(ref_tslog.task_state) == m_etlStateSuccess )
				{
					++c_etl_success;
				}
			}
			else	// 未获取状态
			{
				m_pTaskDB2->SelectTaskScheLogState(ref_tslog);

				// 采集任务是否完成？
				if ( !ref_tslog.end_time.empty() && !ref_tslog.task_state.empty() )
				{
					m_pLog->Output("[YD_TASK] Acquire finished: SEQ=[%d], TASK_ID=[%s], KPI_ID=[%s], ETL_ID=[%s], ETL_TIME=[%s], END_TIME=[%s], TASK_STATE=[%s], STATE_DESC=[%s], REMARK=[%s]", ref_rat.seq_id, ref_tslog.task_id.c_str(), ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.etl_time.c_str(), ref_tslog.end_time.c_str(), ref_tslog.task_state.c_str(), ref_tslog.state_desc.c_str(), ref_tslog.remarks.c_str());

					++c_etl_finished;
					if ( base::PubStr::TrimB(ref_tslog.task_state) == m_etlStateSuccess )
					{
						++c_etl_success;
					}
				}
				else	// 未完成
				{
					long long ll_taskid = 0;
					if ( !base::PubStr::Str2LLong(ref_tslog.task_id, ll_taskid) )
					{
						throw base::Exception(YDTERR_HDL_ETL_TASK, "Convert the task_id (string) to long long failed: SEQ=[%d], KPI_ID=[%s], ETL_ID=[%s], TASK_ID=[%s] [FILE:%s, LINE:%d]", ref_rat.seq_id, ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.task_id.c_str(), __FILE__, __LINE__);
					}

					// 检查采集进程是否异常退出
					if ( !IsProcessAlive(ll_taskid) )		// 进程异常退出
					{
						m_pLog->Output("[YD_TASK] Acquire exited unexpectedly: SEQ=[%d], TASK_ID=[%lld], KPI_ID=[%s], ANA_ID=[%s], ETL_TIME=[%s]", ref_rat.seq_id, ll_taskid, ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.etl_time.c_str());

						ref_tslog.end_time   = base::SimpleTime::Now().Time14();
						ref_tslog.task_state = "ACQUIRE_ABORT";
						ref_tslog.state_desc = "采集进程异常退出";
						m_pTaskDB2->UpdateTaskScheLogState(ref_tslog);

						++c_etl_finished;
					}
				}
			}
		}

		if ( VEC_SIZE == c_etl_success )	// 所有采集全部成功
		{
		}
		else if ( VEC_SIZE == c_etl_finished )		// 所有采集全部完成
		{
		}
		else	// 采集未全部完成
		{
			++it;
		}
	}
}

void YDTask::BuildNewTask() throw(base::Exception)
{
	if ( m_mTaskWait.empty() )
	{
		return;
	}
}

void YDTask::FinishTask() throw(base::Exception)
{
	if ( m_mTaskEnd.empty() )
	{
		return;
	}
}

