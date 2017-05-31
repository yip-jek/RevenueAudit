#include "ydtask.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "config.h"
#include "log.h"
#include "ydtaskdb2.h"

YDTask::YDTask(base::Config& cfg)
:Task(cfg)
,m_minRunTimeInterval(0)
,m_maxTaskScheLogID(0)
,m_taskFinished(0)
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
	m_pCfg->RegisterItem("SYS", "MIN_RUN_TIMEINTERVAL");

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
	m_pCfg->RegisterItem("COMMON", "ETL_IGNORE_ERROR");

	// 读取数据库表配置
	m_pCfg->RegisterItem("TABLE", "TAB_TASK_SCHE");
	m_pCfg->RegisterItem("TABLE", "TAB_TASK_SCHE_LOG");
	m_pCfg->RegisterItem("TABLE", "TAB_KPI_RULE");
	m_pCfg->RegisterItem("TABLE", "TAB_ETL_RULE");

	m_pCfg->ReadConfig();

	m_minRunTimeInterval = m_pCfg->GetCfgLongVal("SYS", "MIN_RUN_TIMEINTERVAL");

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

	// 采集忽略的错误列表
	std::string str_etl_error = m_pCfg->GetCfgValue("COMMON", "ETL_IGNORE_ERROR");
	base::PubStr::Str2StrVector(str_etl_error, "|", m_vecEtlIgnoreError);

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

bool YDTask::DelTaskPauseWithSEQ(const int& seq)
{
	for ( std::list<RATask>::iterator it = m_listTaskPause.begin(); it != m_listTaskPause.end(); ++it )
	{
		if ( seq == it->seq_id )
		{
			m_listTaskPause.erase(it);
			return true;
		}
	}

	return false;
}

void YDTask::Init() throw(base::Exception)
{
	InitConnect();

	// 指定工作目录为Hive代理的路径
	if ( chdir(m_hiveAgentPath.c_str()) < 0 )
	{
		throw base::Exception(YDTERR_INIT, "Change work dir to '%s' failed! %s [FILE:%s, LINE:%d]", m_hiveAgentPath.c_str(), strerror(errno), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Change work dir OK: [%s]", m_hiveAgentPath.c_str());

	if ( !m_pTaskDB2->IsTableExists(m_tabTaskSche) )
	{
		throw base::Exception(YDTERR_INIT, "The task schedule table is not existed: %s [FILE:%s, LINE:%d]", m_tabTaskSche.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Check the task schedule table OK: [%s]", m_tabTaskSche.c_str());

	if ( !m_pTaskDB2->IsTableExists(m_tabTaskScheLog) )
	{
		throw base::Exception(YDTERR_INIT, "The task schedule log table is not existed: %s [FILE:%s, LINE:%d]", m_tabTaskScheLog.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Check the task schedule log table OK: [%s]", m_tabTaskScheLog.c_str());

	if ( !m_pTaskDB2->IsTableExists(m_tabKpiRule) )
	{
		throw base::Exception(YDTERR_INIT, "The kpi rule table is not existed: %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Check the kpi rule table OK: [%s]", m_tabKpiRule.c_str());

	if ( !m_pTaskDB2->IsTableExists(m_tabEtlRule) )
	{
		throw base::Exception(YDTERR_INIT, "The etl rule table is not existed: %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Check the etl rule table OK: [%s]", m_tabEtlRule.c_str());

	if ( m_minRunTimeInterval <= 0 )
	{
		throw base::Exception(YDTERR_INIT, "The min run time interval is invalid: %d [FILE:%s, LINE:%d]", m_minRunTimeInterval, __FILE__, __LINE__);
	}
	m_pLog->Output("[YD_TASK] Check min run time interval OK: [%d]", m_minRunTimeInterval);

	// 当前任务日志LOG_ID最大值
	m_maxTaskScheLogID = m_pTaskDB2->GetTaskScheLogMaxID();
	m_pLog->Output("[YD_TASK] Get the max log ID in task schedule log table: [%d]", m_maxTaskScheLogID);

	m_stLastTaskBuild = base::SimpleTime::Now();
	OutputConfiguration();
	m_pLog->Output("[YD_TASK] Init OK.");
}

void YDTask::OutputConfiguration()
{
	m_pLog->Output("================================================================================");
	m_pLog->Output("[YD_TASK] (CONFIG) HIVE_AGENT_PATH   : [%s]", m_hiveAgentPath.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) PROGRAM_VER       : [%s]", m_binVer.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) PROGRAM_ETL       : [%s]", m_binAcquire.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) ETL_MODE          : [%s]", m_modeAcquire.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) ETL_CFG_PATH      : [%s]", m_cfgAcquire.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) PROGRAM_ANA       : [%s]", m_binAnalyse.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) ANA_MODE          : [%s]", m_modeAnalyse.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) ANA_CFG_PATH      : [%s]", m_cfgAnalyse.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) ETL_SUCCESS_STATE : [%s]", m_etlStateSuccess.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) TASKSCHEDULE_TABLE: [%s]", m_tabTaskSche.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) TASKSCHELOG_TABLE : [%s]", m_tabTaskScheLog.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) KPI_RULE_TABLE    : [%s]", m_tabKpiRule.c_str());
	m_pLog->Output("[YD_TASK] (CONFIG) ETL_RULE_TABLE    : [%s]", m_tabEtlRule.c_str());

	m_pLog->Output("--------------------------------------------------------------------------------");
	const int VEC_SIZE = m_vecEtlIgnoreError.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		m_pLog->Output("[YD_TASK] (CONFIG) ETL_IGNORE_ERROR_%02d: [%s]", (i+1), m_vecEtlIgnoreError[i].c_str());
	}
	m_pLog->Output("================================================================================");
}

bool YDTask::ConfirmQuit()
{
	return (m_mTaskSche.empty()
		&& m_mTaskSche_bak.empty()
		&& m_mTaskWait.empty()
		&& m_listTaskPause.empty()
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
		TaskSchedule& ref_ts = it->second;

		bk_it = m_mTaskSche_bak.find(id);
		if ( bk_it != m_mTaskSche_bak.end() )	// 已存在
		{
			if ( ref_ts != bk_it->second )	// 更新
			{
				if ( rat.LoadFromTaskSche(ref_ts) )
				{
					++c_upd_tasks;

					// 任务更新，旧任务从暂停队列中删除
					DelTaskPauseWithSEQ(id);
					m_mTaskWait[id] = rat;

					m_mTaskSche_bak[id] = ref_ts;
					m_pLog->Output("[YD_TASK] 更新任务: SEQ=[%d], KPI=[%s], TASK_TYPE=[%s], CYCLE=[%s], ETL_TIME=[%s], EXPIRY_DATE_START=[%s], EXPIRY_DATE_END=[%s]", id, rat.kpi_id.c_str(), ref_ts.task_type.c_str(), ref_ts.task_cycle.c_str(), ref_ts.etl_time.c_str(), ref_ts.expiry_date_start.c_str(), ref_ts.expiry_date_end.c_str());
				}
				else	// 错误：载入任务日程失败，不更新
				{
					++c_err_tasks;
					m_pLog->Output("[YD_TASK] 任务日程载入失败，取消更新任务：SEQ=[%d], KPI=[%s], TASK_TYPE=[%s], CYCLE=[%s], ETL_TIME=[%s], EXPIRY_DATE_START=[%s], EXPIRY_DATE_END=[%s]", id, ref_ts.kpi_id.c_str(), ref_ts.task_type.c_str(), ref_ts.task_cycle.c_str(), ref_ts.etl_time.c_str(), ref_ts.expiry_date_start.c_str(), ref_ts.expiry_date_end.c_str());

					// 置为未激活
					m_pTaskDB2->SetTaskScheNotActive(id);
				}
			}
		}
		else	// 不存在
		{
			if ( rat.LoadFromTaskSche(ref_ts) )		// 新增
			{
				++c_new_tasks;
				m_mTaskWait[id] = rat;
				m_mTaskSche_bak[id] = ref_ts;
				m_pLog->Output("[YD_TASK] 新增任务: SEQ=[%d], KPI=[%s], TASK_TYPE=[%s], CYCLE=[%s], ETL_TIME=[%s], EXPIRY_DATE_START=[%s], EXPIRY_DATE_END=[%s]", id, rat.kpi_id.c_str(), ref_ts.task_type.c_str(), ref_ts.task_cycle.c_str(), ref_ts.etl_time.c_str(), ref_ts.expiry_date_start.c_str(), ref_ts.expiry_date_end.c_str());
			}
			else	// 错误：载入任务日程失败，不新增
			{
				++c_err_tasks;
				m_pLog->Output("[YD_TASK] 任务日程载入失败，取消新增任务：SEQ=[%d], KPI=[%s], TASK_TYPE=[%s], CYCLE=[%s], ETL_TIME=[%s], EXPIRY_DATE_START=[%s], EXPIRY_DATE_END=[%s]", id, ref_ts.kpi_id.c_str(), ref_ts.task_type.c_str(), ref_ts.task_cycle.c_str(), ref_ts.etl_time.c_str(), ref_ts.expiry_date_start.c_str(), ref_ts.expiry_date_end.c_str());

				// 置为未激活
				m_pTaskDB2->SetTaskScheNotActive(id);
			}
		}
	}

	// 有变化才输出
	if ( (c_new_tasks + c_upd_tasks + c_err_tasks) > 0 )
	{
		m_pLog->Output("[YD_TASK] 新增任务数: %d", c_new_tasks);
		m_pLog->Output("[YD_TASK] 更新任务数: %d", c_upd_tasks);
		m_pLog->Output("[YD_TASK] 错误任务数: %d", c_err_tasks);
	}
}

void YDTask::DelUnavailableTask()
{
	// 删除不存在或者没有激活的任务
	int c_del_tasks = 0;
	std::map<int, TaskSchedule>::iterator bk_it = m_mTaskSche_bak.begin();
	while ( bk_it != m_mTaskSche_bak.end() )
	{
		int bk_id = bk_it->first;
		TaskSchedule& ref_ts = bk_it->second;
		if ( m_mTaskSche.find(bk_id) == m_mTaskSche.end() )	// 不存在或者没有激活
		{
			if ( m_mTaskWait.find(bk_id) != m_mTaskWait.end() )
			{
				m_pLog->Output("[YD_TASK] 从等待队列中删除不存在或者未激活的任务：SEQ=[%d], KPI=[%s]", bk_id, m_mTaskWait[bk_id].kpi_id.c_str());
				m_mTaskWait.erase(bk_id);
			}

			if ( DelTaskPauseWithSEQ(bk_id) )
			{
				m_pLog->Output("[YD_TASK] 从暂停队列中删除不存在或者未激活的任务：SEQ=[%d]", bk_id);
			}

			if ( m_mEtlTaskRun.find(bk_id) != m_mEtlTaskRun.end()
				|| m_mAnaTaskRun.find(bk_id) != m_mAnaTaskRun.end()
				|| m_mTaskEnd.find(bk_id) != m_mTaskEnd.end() )
			{
				m_pLog->Output("[YD_TASK] 任务正在运行，预定于运行结束后删除：SEQ=[%d], KPI=[%s]", bk_id, ref_ts.kpi_id.c_str());
				m_sDelAfterRun.insert(bk_id);
			}

			++c_del_tasks;
			m_pLog->Output("[YD_TASK] 从日程备份中删除不存在或者未激活的任务: SEQ=[%d], KPI=[%s], TASK_TYPE=[%s], CYCLE=[%s], ETL_TIME=[%s], EXPIRY_DATE_START=[%s], EXPIRY_DATE_END=[%s]", bk_id, ref_ts.kpi_id.c_str(), ref_ts.task_type.c_str(), ref_ts.task_cycle.c_str(), ref_ts.etl_time.c_str(), ref_ts.expiry_date_start.c_str(), ref_ts.expiry_date_end.c_str());
			m_mTaskSche_bak.erase(bk_it++);
		}
		else
		{
			++bk_it;
		}
	}

	// 有变化才输出
	if ( c_del_tasks > 0 )
	{
		m_pLog->Output("[YD_TASK] 删除任务数: %d", c_del_tasks);
	}
}

void YDTask::GetNoTask() throw(base::Exception)
{
	m_mTaskSche.clear();
	m_mTaskSche_bak.clear();
	m_mTaskWait.clear();
	m_listTaskPause.clear();
}

void YDTask::ShowTasksInfo()
{
	m_pLog->Output(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
	m_pLog->Output("[YD_TASK] 任务日程数: %llu", m_mTaskSche.size());
	m_pLog->Output("[YD_TASK] 日程备份数: %llu", m_mTaskSche_bak.size());
	m_pLog->Output("[YD_TASK] 任务等待数: %llu", m_mTaskWait.size());
	m_pLog->Output("[YD_TASK] 任务暂停数: %llu", m_listTaskPause.size());
	m_pLog->Output("[YD_TASK] 采集任务数: %llu", m_mEtlTaskRun.size());
	m_pLog->Output("[YD_TASK] 分析任务数: %llu", m_mAnaTaskRun.size());
	m_pLog->Output("[YD_TASK] 任务完成数: %d", m_taskFinished);
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
		RATask&      ref_rat   = it->second;
		TaskScheLog& ref_tslog = ref_rat.tsl_ana_task;

		long long ll_taskid = 0;
		if ( !base::PubStr::Str2LLong(ref_tslog.task_id, ll_taskid) )
		{
			throw base::Exception(YDTERR_HDL_ANA_TASK, "Convert the task_id (string) to long long type failed: SEQ=[%d], KPI=[%s], ANA_ID=[%s], TASK_ID=[%s] [FILE:%s, LINE:%d]", ref_rat.seq_id, ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.task_id.c_str(), __FILE__, __LINE__);
		}

		// 检查分析进程是否已经退出
		if ( IsProcessAlive(ll_taskid) )	// 进程是活动的
		{
			++it;
		}
		else	// 进程已退出
		{
			// 分析任务是否已完成？
			m_pTaskDB2->SelectTaskScheLogState(ref_tslog);
			if ( !ref_tslog.end_time.empty() && !ref_tslog.task_state.empty() )	// 分析已完成
			{
				m_pLog->Output("[YD_TASK] Analyse finished: SEQ=[%d], LOG=[%d], TASK_ID=[%s], KPI=[%s], ANA_ID=[%s], ETL_TIME=[%s], END_TIME=[%s], TASK_STATE=[%s], STATE_DESC=[%s], REMARK=[%s]", ref_rat.seq_id, ref_tslog.log_id, ref_tslog.task_id.c_str(), ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.etl_time.c_str(), ref_tslog.end_time.c_str(), ref_tslog.task_state.c_str(), ref_tslog.state_desc.c_str(), ref_tslog.remarks.c_str());

				m_mTaskEnd[ref_rat.seq_id] = ref_rat;
				m_mAnaTaskRun.erase(it++);
			}
			else	// 分析异常 (未完成)
			{
				m_pLog->Output("[YD_TASK] Analyse exited unexpectedly: SEQ=[%d], LOG=[%d], TASK_ID=[%lld], KPI=[%s], ANA_ID=[%s], ETL_TIME=[%s]", ref_rat.seq_id, ref_tslog.log_id, ll_taskid, ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.etl_time.c_str());

				ref_tslog.end_time   = base::SimpleTime::Now().Time14();
				ref_tslog.task_state = "ANA_ABORT";
				ref_tslog.state_desc = "分析进程异常退出";
				m_pTaskDB2->UpdateTaskScheLog(ref_tslog);

				m_mTaskEnd[ref_rat.seq_id] = ref_rat;
				m_mAnaTaskRun.erase(it++);
			}
		}
	}
}

void YDTask::HandleEtlTask() throw(base::Exception)
{
	if ( m_mEtlTaskRun.empty() )
	{
		return;
	}

	// 获取分析任务状态
	std::string str_timenow;
	std::map<int, RATask>::iterator it = m_mEtlTaskRun.begin();
	while ( it != m_mEtlTaskRun.end() )
	{
		RATask& ref_rat = it->second;

		int c_etl_success  = 0;
		int c_etl_finished = 0;
		const int VEC_SIZE = ref_rat.vec_etl_tasks.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			TaskScheLog& ref_tslog = ref_rat.vec_etl_tasks[i];
			if ( !ref_tslog.end_time.empty() && !ref_tslog.task_state.empty() )	// 已获取状态
			{
				++c_etl_finished;
				if ( IsEtlSucceeded(ref_tslog) )
				{
					++c_etl_success;
				}
			}
			else	// 未获取状态
			{
				long long ll_taskid = 0;
				if ( !base::PubStr::Str2LLong(ref_tslog.task_id, ll_taskid) )
				{
					throw base::Exception(YDTERR_HDL_ETL_TASK, "Convert the task_id (string) to long long type failed: SEQ=[%d], KPI=[%s], ETL_ID=[%s], TASK_ID=[%s] [FILE:%s, LINE:%d]", ref_rat.seq_id, ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.task_id.c_str(), __FILE__, __LINE__);
				}

				// 检查采集进程是否已经退出
				// 如果进程为活动的，则 DO NOTHING!
				if ( !IsProcessAlive(ll_taskid) )	// 进程已退出
				{
					// 采集任务是否已完成？
					m_pTaskDB2->SelectTaskScheLogState(ref_tslog);
					if ( !ref_tslog.end_time.empty() && !ref_tslog.task_state.empty() )	// 采集已完成
					{
						m_pLog->Output("[YD_TASK] Acquire finished: SEQ=[%d], LOG=[%d], TASK_ID=[%s], KPI=[%s], ETL_ID=[%s], ETL_TIME=[%s], END_TIME=[%s], TASK_STATE=[%s], STATE_DESC=[%s], REMARK=[%s]", ref_rat.seq_id, ref_tslog.log_id, ref_tslog.task_id.c_str(), ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.etl_time.c_str(), ref_tslog.end_time.c_str(), ref_tslog.task_state.c_str(), ref_tslog.state_desc.c_str(), ref_tslog.remarks.c_str());

						++c_etl_finished;
						if ( IsEtlSucceeded(ref_tslog) )
						{
							++c_etl_success;
						}
					}
					else	// 采集异常 (未完成)
					{
						m_pLog->Output("[YD_TASK] Acquire exited unexpectedly: SEQ=[%d], LOG=[%d], TASK_ID=[%lld], KPI=[%s], ETL_ID=[%s], ETL_TIME=[%s]", ref_rat.seq_id, ref_tslog.log_id, ll_taskid, ref_rat.kpi_id.c_str(), ref_tslog.sub_id.c_str(), ref_tslog.etl_time.c_str());

						ref_tslog.end_time   = base::SimpleTime::Now().Time14();
						ref_tslog.task_state = "ETL_ABORT";
						ref_tslog.state_desc = "采集进程异常退出";
						m_pTaskDB2->UpdateTaskScheLog(ref_tslog);

						++c_etl_finished;
					}
				}
			}
		}

		TaskScheLog& ref_tslog = ref_rat.tsl_ana_task;
		if ( VEC_SIZE == c_etl_success )	// 所有采集全部成功
		{
			m_pLog->Output("[YD_TASK] Acquire all succeed: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s], ETL_TIME=[%s]", ref_rat.seq_id, ref_tslog.task_type.c_str(), ref_rat.kpi_id.c_str(), ref_tslog.etl_time.c_str());

			// 下发分析任务
			m_pLog->Output("[YD_TASK] 采集任务全部成功，准备下发分析任务");
			CreateTask(ref_tslog);

			m_mAnaTaskRun[ref_rat.seq_id] = ref_rat;
			m_mEtlTaskRun.erase(it++);
		}
		else if ( VEC_SIZE == c_etl_finished )	// 所有采集全部完成，但不是全部成功
		{
			m_pLog->Output("[YD_TASK] Acquire all finished: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s], ETL_TIME=[%s]", ref_rat.seq_id, ref_tslog.task_type.c_str(), ref_rat.kpi_id.c_str(), ref_tslog.etl_time.c_str());

			// 任务失败：停止下发分析任务
			m_pLog->Output("[YD_TASK] 采集任务全部完成，但没有全部成功，此轮任务失败：只记录任务日程日志，不下发分析任务！");
			str_timenow          = base::SimpleTime::Now().Time14();
			ref_tslog.log_id     = ++m_maxTaskScheLogID;
			ref_tslog.task_id    = "0";
			ref_tslog.start_time = str_timenow;
			ref_tslog.end_time   = str_timenow;
			ref_tslog.task_state = "ANA_IGNORE";
			ref_tslog.state_desc = "不进行分析";
			ref_tslog.remarks    = "采集未全部成功，任务失败！停止下发分析任务！";
			m_pTaskDB2->InsertTaskScheLog(ref_tslog);

			// 任务结束
			m_mTaskEnd[ref_rat.seq_id] = ref_rat;
			m_mEtlTaskRun.erase(it++);
		}
		else	// 采集未全部完成
		{
			++it;
		}
	}
}

bool YDTask::IsEtlSucceeded(const TaskScheLog& ts_log) const
{
	// 采集结果状态为成功，或是可以忽略的错误
	if ( base::PubStr::TrimB(ts_log.task_state) != m_etlStateSuccess )
	{
		const int VEC_SIZE = m_vecEtlIgnoreError.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			if ( ts_log.remarks.find(m_vecEtlIgnoreError[i]) != std::string::npos )
			{
				return true;
			}
		}

		return false;
	}

	return true;
}

void YDTask::BuildNewTask() throw(base::Exception)
{
	if ( m_mTaskWait.empty() && m_listTaskPause.empty() )
	{
		return;
	}

	const base::SimpleTime ST_NOW(base::SimpleTime::Now());

	// 尝试从任务暂停队列中恢复任务
	std::list<RATask>::iterator curr_it;
	for ( std::list<RATask>::iterator it = m_listTaskPause.begin(); it != m_listTaskPause.end(); )
	{
		curr_it = it++;

		if ( PerformNewTask(ST_NOW, *curr_it, true) >= 0 )
		{
			m_listTaskPause.erase(curr_it);
		}
	}

	// 检查任务等待队列
	std::map<int, RATask>::iterator curr_m_it;
	for ( std::map<int, RATask>::iterator m_it = m_mTaskWait.begin(); m_it != m_mTaskWait.end(); )
	{
		curr_m_it = m_it++;

		RATask& ref_rat = curr_m_it->second;

		// 任务未到达有效期
		if ( ST_NOW < ref_rat.st_expiry_start )
		{
			continue;
		}

		// 任务过期
		if ( ref_rat.st_expiry_end < ST_NOW )
		{
			m_pLog->Output("[YD_TASK] Task expired: SEQ=[%d], KPI=[%s], TASK_TYPE=[%s], EXPIRY_DATE_START=[%s], EXPIRY_DATE_END=[%s]", ref_rat.seq_id, ref_rat.kpi_id.c_str(), (RATask::TTYPE_P==ref_rat.type?"P":"T"), ref_rat.st_expiry_start.TimeStamp().c_str(), ref_rat.st_expiry_end.TimeStamp().c_str());

			// 置为未激活
			m_pTaskDB2->SetTaskScheNotActive(ref_rat.seq_id);
			m_mTaskWait.erase(curr_m_it);
			continue;
		}

		if ( PerformNewTask(ST_NOW, ref_rat, false) >= 0 )
		{
			m_mTaskWait.erase(curr_m_it);
		}
	}

	m_stLastTaskBuild = ST_NOW;
}

int YDTask::PerformNewTask(const base::SimpleTime& st_now, RATask& rat, bool task_continue)
{
	// 尝试恢复任务：同类型的任务仍在运行？
	if ( task_continue && CheckSameKindRunningTask(rat) )
	{
		return -1;
	}

	std::string str_msg;
	std::string str_etl_time;
	std::string str_etl_id;
	std::string str_ana_id;
	std::string str_task_type = (RATask::TTYPE_P == rat.type) ? "P" : "T";
	std::vector<std::string> vec_etl_id;

	// 是否有下一个采集时间点？
	if ( rat.et_etl_time.HaveNext() )
	{
		// 是否有同类型的任务在运行？
		if ( !task_continue && CheckSameKindRunningTask(rat, &str_msg) )
		{
			m_pLog->Output("[YD_TASK] %s", str_msg.c_str());
			m_listTaskPause.push_back(rat);
			m_pLog->Output("[YD_TASK] Task pause: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s]", rat.seq_id, str_task_type.c_str(), rat.kpi_id.c_str());
			return rat.seq_id;
		}

		// 获得下一个采集时间点
		rat.et_etl_time.GetNext(str_etl_time);
		rat.st_task_start = base::SimpleTime::Now();
		rat.st_task_finish.Set(0);
		m_pLog->Output("[YD_TASK] Task continue: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s], ETL_TIME=[%s], TASK_START_TIME=[%s]", rat.seq_id, str_task_type.c_str(), rat.kpi_id.c_str(), str_etl_time.c_str(), rat.st_task_start.TimeStamp().c_str());

		// 更新任务时间
		m_pTaskDB2->UpdateTaskScheTaskTime(rat.seq_id, rat.st_task_start.Time14(), "");

		// 下发采集任务
		const int VEC_SIZE = rat.vec_etl_tasks.size();
		m_pLog->Output("[YD_TASK] 需要下发的采集任务数：%d", VEC_SIZE);
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			TaskScheLog& ref_tslog = rat.vec_etl_tasks[i];
			ref_tslog.etl_time = str_etl_time;
			ref_tslog.end_time.clear();
			ref_tslog.task_state.clear();
			ref_tslog.state_desc.clear();
			ref_tslog.remarks.clear();

			// 创建任务
			CreateTask(ref_tslog);
		}

		// 重置分析任务
		rat.tsl_ana_task.etl_time = str_etl_time;
		rat.tsl_ana_task.end_time.clear();
		rat.tsl_ana_task.task_state.clear();
		rat.tsl_ana_task.state_desc.clear();
		rat.tsl_ana_task.remarks.clear();

		// 将任务插入到运行队列
		m_mEtlTaskRun[rat.seq_id] = rat;
		return rat.seq_id;
	}
	else	// 没有采集时间点
	{
		// 任务是否已经执行过？
		if ( !rat.st_task_finish.IsValid() )		// 未执行
		{
			// 尝试恢复任务时，直接下发任务
			// 否则，判断是否到达运行周期
			if ( task_continue || (!task_continue && rat.cycle.IsTimeRange(m_stLastTaskBuild, st_now)) )
			{
				// 是否有同类型的任务在运行？
				if ( !task_continue && CheckSameKindRunningTask(rat, &str_msg) )
				{
					m_pLog->Output("[YD_TASK] %s", str_msg.c_str());
					m_listTaskPause.push_back(rat);
					m_pLog->Output("[YD_TASK] Task pause: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s]", rat.seq_id, str_task_type.c_str(), rat.kpi_id.c_str());
					return rat.seq_id;
				}

				rat.et_etl_time.Init();
				if ( !rat.et_etl_time.HaveNext() )		// 没有首个采集时间点
				{
					throw base::Exception(YDTERR_BUILD_NEWTASK, "Build new task failed: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s]. Can't get the first ETL time! [FILE:%s, LINE:%d]", rat.seq_id, str_task_type.c_str(), rat.kpi_id.c_str(), __FILE__, __LINE__);
				}

				rat.et_etl_time.GetNext(str_etl_time);
				rat.st_task_start = base::SimpleTime::Now();
				m_pLog->Output("[YD_TASK] Task start: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s], ETL_TIME=[%s], TASK_START_TIME=[%s]", rat.seq_id, str_task_type.c_str(), rat.kpi_id.c_str(), str_etl_time.c_str(), rat.st_task_start.TimeStamp().c_str());

				// 更新任务时间
				m_pTaskDB2->UpdateTaskScheTaskTime(rat.seq_id, rat.st_task_start.Time14(), "");

				// 获取采集ID与分析ID
				if ( !m_pTaskDB2->GetKpiRuleSubID(rat.kpi_id, str_etl_id, str_ana_id) )
				{
					m_pLog->Output("[YD_TASK] <ERROR> Build new task failed! Can't get sub ID of kpi rule: SEQ=[%d], KPI=[%s], TASK_TYPE=[%s]", rat.seq_id, rat.kpi_id.c_str(), str_task_type.c_str());

					// 置为未激活
					m_pTaskDB2->SetTaskScheNotActive(rat.seq_id);
					return rat.seq_id;
				}
				base::PubStr::Str2StrVector(str_etl_id, "|", vec_etl_id);

				// 下发新的采集任务
				TaskScheLog ts_log;
				const int VEC_SIZE = vec_etl_id.size();
				m_pLog->Output("[YD_TASK] 需要下发的采集任务数：%d", VEC_SIZE);
				for ( int i = 0; i < VEC_SIZE; ++i )
				{
					ts_log.Clear();
					ts_log.kpi_id    = rat.kpi_id;
					ts_log.sub_id    = vec_etl_id[i];
					ts_log.task_type = str_task_type;
					ts_log.etl_time  = str_etl_time;
					ts_log.app_type  = TaskScheLog::S_APP_TYPE_ETL;

					// 创建任务
					CreateTask(ts_log);

					rat.vec_etl_tasks.push_back(ts_log);
				}

				// 配置新的分析任务
				ts_log.Clear();
				ts_log.kpi_id        = rat.kpi_id;
				ts_log.sub_id        = str_ana_id;
				ts_log.task_type     = str_task_type;
				ts_log.etl_time      = str_etl_time;
				ts_log.app_type      = TaskScheLog::S_APP_TYPE_ANA;
				rat.tsl_ana_task = ts_log;

				// 将任务插入到运行队列
				m_mEtlTaskRun[rat.seq_id] = rat;
				return rat.seq_id;
			}
		}
		else	// 已执行
		{
			if ( RATask::TTYPE_P == rat.type )	// 常驻任务
			{
				// 是否超过最小时间间隔？
				if ( IsOverMinTimeInterval(rat.st_task_finish) )
				{
					// 任务重置，但采集时间不重置
					rat.st_task_start.Set(0);
					rat.st_task_finish.Set(0);
					std::vector<TaskScheLog>().swap(rat.vec_etl_tasks);
					rat.tsl_ana_task.Clear();
				}
			}
			else	// 临时任务：只执行一次
			{
				m_pLog->Output("[YD_TASK] Temporary task run only once: SEQ=[%d], KPI=[%s], TASK_TYPE=[%s]", rat.seq_id, rat.kpi_id.c_str(), str_task_type.c_str());

				// 置为未激活
				m_pTaskDB2->SetTaskScheNotActive(rat.seq_id);
				return rat.seq_id;
			}
		}
	}

	return -1;
}

void YDTask::CreateTask(TaskScheLog& ts_log)
{
	// 插入任务日志
	ts_log.log_id     = ++m_maxTaskScheLogID;
	ts_log.task_id    = base::PubStr::LLong2Str(GenerateTaskID());
	ts_log.start_time = base::SimpleTime::Now().Time14();
	m_pTaskDB2->InsertTaskScheLog(ts_log);

	std::string command;
	if ( TaskScheLog::S_APP_TYPE_ETL == ts_log.app_type )		// 采集任务
	{
		m_pLog->Output("[YD_TASK] Create acquire task: LOG=[%d], KPI=[%s], ETL_ID=[%s], TASK_ID=[%s], TASK_TYPE=[%s], ETL_TIME=[%s], START_TIME=[%s]", ts_log.log_id, ts_log.kpi_id.c_str(), ts_log.sub_id.c_str(), ts_log.task_id.c_str(), ts_log.task_type.c_str(), ts_log.etl_time.c_str(), ts_log.start_time.c_str());

		// 更新采集时间
		m_pTaskDB2->UpdateEtlTime(ts_log.sub_id, EtlTime::Convert(ts_log.etl_time));

		// 休眠 1 秒，避免采集程序拉起得过于密集导致重建采集结果表失败
		sleep(1);

		// 采集程序：守护进程
		base::PubStr::SetFormatString(command, "%s 1 %s %s %s %s 00001:%s:%s:%d:", m_binAcquire.c_str(), ts_log.task_id.c_str(), m_modeAcquire.c_str(), m_binVer.c_str(), m_cfgAcquire.c_str(), ts_log.kpi_id.c_str(), ts_log.sub_id.c_str(), ts_log.log_id);
	}
	else if ( TaskScheLog::S_APP_TYPE_ANA == ts_log.app_type )	// 分析任务
	{
		m_pLog->Output("[YD_TASK] Create analyse task: LOG=[%d], KPI=[%s], ANA_ID=[%s], TASK_ID=[%s], TASK_TYPE=[%s], ETL_TIME=[%s], START_TIME=[%s]", ts_log.log_id, ts_log.kpi_id.c_str(), ts_log.sub_id.c_str(), ts_log.task_id.c_str(), ts_log.task_type.c_str(), ts_log.etl_time.c_str(), ts_log.start_time.c_str());

		// 分析程序：守护进程
		base::PubStr::SetFormatString(command, "%s 1 %s %s %s %s 00001:%s:%s:%d:", m_binAnalyse.c_str(), ts_log.task_id.c_str(), m_modeAnalyse.c_str(), m_binVer.c_str(), m_cfgAnalyse.c_str(), ts_log.kpi_id.c_str(), ts_log.sub_id.c_str(), ts_log.log_id);
	}
	else
	{
		throw base::Exception(YDTERR_CREATE_TASK, "Create task failed: LOG=[%d], KPI=[%s], SUB=[%s], TASK_ID=[%s], TASK_TYPE=[%s], ETL_TIME=[%s], START_TIME=[%s]. Unknown app type: %s [FILE:%s, LINE:%d]", ts_log.log_id, ts_log.kpi_id.c_str(), ts_log.sub_id.c_str(), ts_log.task_id.c_str(), ts_log.task_type.c_str(), ts_log.etl_time.c_str(), ts_log.start_time.c_str(), ts_log.app_type.c_str(), __FILE__, __LINE__);
	}

	// 启动进程
	m_pLog->Output("[YD_TASK] Execute: %s", command.c_str());
	system(command.c_str());
}

bool YDTask::CheckSameKindRunningTask(const RATask& rat, std::string* pMsg /*= NULL*/)
{
	// 存在同序号的任务？
	std::map<int, RATask>::iterator it;
	if ( (it = m_mEtlTaskRun.find(rat.seq_id)) != m_mEtlTaskRun.end()
		|| (it = m_mAnaTaskRun.find(rat.seq_id)) != m_mAnaTaskRun.end()
		|| (it = m_mTaskEnd.find(rat.seq_id)) != m_mTaskEnd.end() )
	{
		RATask& ref_rat = it->second;

		if ( pMsg != NULL )
		{
			base::PubStr::SetFormatString(*pMsg, "Same SEQ task is running: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s], ETL_TIME=[%s], TASK_START_TIME=[%s]", ref_rat.seq_id, ref_rat.tsl_ana_task.task_type.c_str(), ref_rat.kpi_id.c_str(), ref_rat.tsl_ana_task.etl_time.c_str(), ref_rat.st_task_start.TimeStamp().c_str());
		}
		return true;
	}

	// 存在同指标的任务？
	for ( it = m_mEtlTaskRun.begin(); it != m_mEtlTaskRun.end(); ++it )
	{
		if ( rat.kpi_id == it->second.kpi_id )
		{
			RATask& ref_rat = it->second;

			if ( pMsg != NULL )
			{
				base::PubStr::SetFormatString(*pMsg, "Same KPI task is running: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s], ETL_TIME=[%s], TASK_START_TIME=[%s]", ref_rat.seq_id, ref_rat.tsl_ana_task.task_type.c_str(), ref_rat.kpi_id.c_str(), ref_rat.tsl_ana_task.etl_time.c_str(), ref_rat.st_task_start.TimeStamp().c_str());
			}
			return true;
		}
	}

	// 存在同指标的任务？
	for ( it = m_mAnaTaskRun.begin(); it != m_mAnaTaskRun.end(); ++it )
	{
		if ( rat.kpi_id == it->second.kpi_id )
		{
			RATask& ref_rat = it->second;

			if ( pMsg != NULL )
			{
				base::PubStr::SetFormatString(*pMsg, "Same KPI task is running: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s], ETL_TIME=[%s], TASK_START_TIME=[%s]", ref_rat.seq_id, ref_rat.tsl_ana_task.task_type.c_str(), ref_rat.kpi_id.c_str(), ref_rat.tsl_ana_task.etl_time.c_str(), ref_rat.st_task_start.TimeStamp().c_str());
			}
			return true;
		}
	}

	// 存在同指标的任务？
	for ( it = m_mTaskEnd.begin(); it != m_mTaskEnd.end(); ++it )
	{
		if ( rat.kpi_id == it->second.kpi_id )
		{
			RATask& ref_rat = it->second;

			if ( pMsg != NULL )
			{
				base::PubStr::SetFormatString(*pMsg, "Same KPI task is running: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s], ETL_TIME=[%s], TASK_START_TIME=[%s]", ref_rat.seq_id, ref_rat.tsl_ana_task.task_type.c_str(), ref_rat.kpi_id.c_str(), ref_rat.tsl_ana_task.etl_time.c_str(), ref_rat.st_task_start.TimeStamp().c_str());
			}
			return true;
		}
	}

	return false;
}

bool YDTask::IsOverMinTimeInterval(const base::SimpleTime& st_time) const
{
	const base::SimpleTime ST_NOW(base::SimpleTime::Now());
	long min_off = (ST_NOW.GetHour() - st_time.GetHour()) * 60;
	min_off += (ST_NOW.GetMin() - st_time.GetMin());
	min_off += base::PubTime::DayDifference(st_time, ST_NOW) * 1440;
	return (min_off > m_minRunTimeInterval);
}

void YDTask::FinishTask() throw(base::Exception)
{
	if ( m_mTaskEnd.empty() )
	{
		return;
	}

	for ( std::map<int, RATask>::iterator it = m_mTaskEnd.begin(); it != m_mTaskEnd.end(); ++it )
	{
		RATask& ref_rat = it->second;
		ref_rat.st_task_finish = base::SimpleTime::Now();
		m_pLog->Output("[YD_TASK] Task finished: SEQ=[%d], TASK_TYPE=[%s], KPI=[%s], ETL_TIME=[%s], TASK_START_TIME=[%s], TASK_END_TIME=[%s]", ref_rat.seq_id, ref_rat.tsl_ana_task.task_type.c_str(), ref_rat.kpi_id.c_str(), ref_rat.tsl_ana_task.etl_time.c_str(), ref_rat.st_task_start.TimeStamp().c_str(), ref_rat.st_task_finish.TimeStamp().c_str());

		if ( m_pTaskDB2->IsTaskScheExist(ref_rat.seq_id) )
		{
			m_pTaskDB2->UpdateTaskScheTaskTime(ref_rat.seq_id, ref_rat.st_task_start.Time14(), ref_rat.st_task_finish.Time14());
		}
		else
		{
			m_pLog->Output("[YD_TASK] <WARNING> Task doesn't exist in task schedule table: SEQ=[%d], KPI=[%s]", ref_rat.seq_id);
		}

		// 在删除队列中，则直接永久删除，不再回归等待队列
		if ( m_sDelAfterRun.find(ref_rat.seq_id) != m_sDelAfterRun.end() )
		{
			m_pLog->Output("[YD_TASK] 任务已预定删除，现执行删除操作：SEQ=[%d], TASK_TYPE=[%s], KPI=[%s]", ref_rat.seq_id, ref_rat.tsl_ana_task.task_type.c_str(), ref_rat.kpi_id.c_str());
			m_sDelAfterRun.erase(ref_rat.seq_id);
		}
		// 等待队列已存在更新的任务，则直接永久删除
		else if ( m_mTaskWait.find(ref_rat.seq_id) != m_mTaskWait.end() )
		{
			m_pLog->Output("[YD_TASK] 任务已更新，因此删除旧任务：SEQ=[%d], TASK_TYPE=[%s], KPI=[%s]", ref_rat.seq_id, ref_rat.tsl_ana_task.task_type.c_str(), ref_rat.kpi_id.c_str());
		}
		// 否则，回归到等待队列中
		else
		{
			m_mTaskWait[ref_rat.seq_id] = ref_rat;
		}
	}

	// 统计任务完成数
	m_taskFinished += m_mTaskEnd.size();

	// 清空任务完成队列
	m_mTaskEnd.clear();
}

