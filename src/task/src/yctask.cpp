#include "yctask.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "config.h"
#include "log.h"
#include "pubstr.h"
#include "pubtime.h"
#include "simpletime.h"
#include "taskdb2.h"


YCTask::YCTask(base::Config& cfg)
:Task(cfg)
,m_pTaskDB(NULL)
{
	// 日志文件前缀
	base::Log::SetLogFilePrefix("YCTask");
}

YCTask::~YCTask()
{
	ReleaseDB();
}

std::string YCTask::Version()
{
	return ("YCTask: "+Task::Version());
}

void YCTask::ReleaseDB()
{
	if ( m_pTaskDB != NULL )
	{
		delete m_pTaskDB;
		m_pTaskDB = NULL;
	}
}

void YCTask::Init() throw(base::Exception)
{
	InitConnect();

	Check();

	m_pLog->Output("[YC_TASK] Init OK.");
}

void YCTask::LoadConfig() throw(base::Exception)
{
	// 读取任务配置
	m_pCfg->RegisterItem("DATABASE", "DB_NAME");
	m_pCfg->RegisterItem("DATABASE", "USER_NAME");
	m_pCfg->RegisterItem("DATABASE", "PASSWORD");

	m_pCfg->RegisterItem("COMMON", "HIVE_AGENT_PATH");
	m_pCfg->RegisterItem("COMMON", "BIN_VER");
	m_pCfg->RegisterItem("COMMON", "ACQUIRE_BIN");
	m_pCfg->RegisterItem("COMMON", "ACQUIRE_MODE");
	m_pCfg->RegisterItem("COMMON", "ACQUIRE_CONFIG");
	m_pCfg->RegisterItem("COMMON", "ANALYSE_BIN");
	m_pCfg->RegisterItem("COMMON", "ANALYSE_MODE");
	m_pCfg->RegisterItem("COMMON", "ANALYSE_CONFIG");

	m_pCfg->RegisterItem("STATE", "STATE_TASK_BEG");
	m_pCfg->RegisterItem("STATE", "STATE_TASK_ETL_EXP");
	m_pCfg->RegisterItem("STATE", "STATE_TASK_ANA_BEG");
	m_pCfg->RegisterItem("STATE", "STATE_TASK_ANA_EXP");
	m_pCfg->RegisterItem("STATE", "STATE_TASK_END");
	m_pCfg->RegisterItem("STATE", "ETL_END_STATE");
	m_pCfg->RegisterItem("STATE", "ETL_ERROR_STATE");
	m_pCfg->RegisterItem("STATE", "ANA_END_STATE");
	m_pCfg->RegisterItem("STATE", "ANA_ERROR_STATE");

	m_pCfg->RegisterItem("TABLE", "TAB_TASK_REQUEST");
	m_pCfg->RegisterItem("TABLE", "TAB_KPI_RULE");
	m_pCfg->RegisterItem("TABLE", "TAB_ETL_RULE");

	m_pCfg->ReadConfig();


	m_dbInfo.db_inst = m_pCfg->GetCfgValue("DATABASE", "DB_NAME");
	m_dbInfo.db_user = m_pCfg->GetCfgValue("DATABASE", "USER_NAME");
	m_dbInfo.db_pwd  = m_pCfg->GetCfgValue("DATABASE", "PASSWORD");

	m_hiveAgentPath = m_pCfg->GetCfgValue("COMMON", "HIVE_AGENT_PATH");
	m_binVer        = m_pCfg->GetCfgValue("COMMON", "BIN_VER");
	m_binAcquire    = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_BIN");
	m_modeAcquire   = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_MODE");
	m_cfgAcquire    = m_pCfg->GetCfgValue("COMMON", "ACQUIRE_CONFIG");
	m_binAnalyse    = m_pCfg->GetCfgValue("COMMON", "ANALYSE_BIN");
	m_modeAnalyse   = m_pCfg->GetCfgValue("COMMON", "ANALYSE_MODE");
	m_cfgAnalyse    = m_pCfg->GetCfgValue("COMMON", "ANALYSE_CONFIG");

	m_stateTaskBeg      = m_pCfg->GetCfgValue("STATE", "STATE_TASK_BEG");
	m_stateEtlException = m_pCfg->GetCfgValue("STATE", "STATE_TASK_ETL_EXP");
	m_stateAnaBeg       = m_pCfg->GetCfgValue("STATE", "STATE_TASK_ANA_BEG");
	m_stateAnaException = m_pCfg->GetCfgValue("STATE", "STATE_TASK_ANA_EXP");
	m_stateTaskEnd      = m_pCfg->GetCfgValue("STATE", "STATE_TASK_END");
	m_etlStateEnd       = m_pCfg->GetCfgValue("STATE", "ETL_END_STATE");
	m_etlStateError     = m_pCfg->GetCfgValue("STATE", "ETL_ERROR_STATE");
	m_anaStateEnd       = m_pCfg->GetCfgValue("STATE", "ANA_END_STATE");
	m_anaStateError     = m_pCfg->GetCfgValue("STATE", "ANA_ERROR_STATE");

	m_tabTaskReq = m_pCfg->GetCfgValue("TABLE", "TAB_TASK_REQUEST");
	m_tabKpiRule = m_pCfg->GetCfgValue("TABLE", "TAB_KPI_RULE");
	m_tabEtlRule = m_pCfg->GetCfgValue("TABLE", "TAB_ETL_RULE");
}

void YCTask::InitConnect() throw(base::Exception)
{
	ReleaseDB();

	m_pTaskDB = new TaskDB2(m_dbInfo);
	m_pTaskDB->SetTabTaskRequest(m_tabTaskReq);
	m_pTaskDB->SetTabKpiRule(m_tabKpiRule);
	m_pTaskDB->SetTabEtlRule(m_tabEtlRule);
	m_pTaskDB->Connect();
}

void YCTask::Check() throw(base::Exception)
{
	// 指定工作目录为Hive代理的路径
	if ( chdir(m_hiveAgentPath.c_str()) < 0 )
	{
		throw base::Exception(TERROR_CHECK, "Change work dir to '%s' failed! %s [FILE:%s, LINE:%d]", m_hiveAgentPath.c_str(), strerror(errno), __FILE__, __LINE__);
	}
	m_pLog->Output("[YC_TASK] Change work dir to '%s' OK.", m_hiveAgentPath.c_str());

	if ( !m_pTaskDB->IsTableExists(m_tabTaskReq) )
	{
		throw base::Exception(TERROR_CHECK, "The task request table is not existed: %s [FILE:%s, LINE:%d]", m_tabTaskReq.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YC_TASK] Check the task request table OK.");

	if ( !m_pTaskDB->IsTableExists(m_tabKpiRule) )
	{
		throw base::Exception(TERROR_CHECK, "The kpi rule table is not existed: %s [FILE:%s, LINE:%d]", m_tabKpiRule.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YC_TASK] Check the kpi rule table OK.");

	if ( !m_pTaskDB->IsTableExists(m_tabEtlRule) )
	{
		throw base::Exception(TERROR_CHECK, "The etl rule table is not existed: %s [FILE:%s, LINE:%d]", m_tabEtlRule.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[YC_TASK] Check the etl rule table OK.");
}

bool YCTask::ConfirmQuit()
{
	if ( m_vecNewTask.empty()
		&& m_vecEndTask.empty()
		&& m_mTaskReqInfo.empty()
		&& m_vecEtlTaskInfo.empty()
		&& m_vecAnaTaskInfo.empty() )
	{
		return true;
	}

	return false;
}

void YCTask::GetNewTask() throw(base::Exception)
{
	m_pTaskDB->SelectNewTaskRequest(m_vecNewTask);

	if ( !m_vecNewTask.empty() )
	{
		// 更新任务状态
		const int VEC_NEW_TASK_SIZE = m_vecNewTask.size();
		m_pLog->Output("[YC_TASK] Get the new task size: %d", VEC_NEW_TASK_SIZE);

		for ( int i = 0; i < VEC_NEW_TASK_SIZE; ++i )
		{
			TaskRequestUpdate(TSTS_Start, m_vecNewTask[i]);
		}
	}
}

void YCTask::ShowTasksInfo()
{
	m_pLog->Output(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
	m_pLog->Output("[YC_TASK] 指标规则数: %llu", m_mKpiRuleInfo.size());
	m_pLog->Output("[YC_TASK] 新建任务数: %llu", m_vecNewTask.size());
	m_pLog->Output("[YC_TASK] 执行任务数: %llu", m_mTaskReqInfo.size());
	m_pLog->Output("[YC_TASK] 采集任务数: %llu", m_vecEtlTaskInfo.size());
	m_pLog->Output("[YC_TASK] 分析任务数: %llu", m_vecAnaTaskInfo.size());
	m_pLog->Output("[YC_TASK] 完成任务数: %llu", m_vecEndTask.size());
	m_pLog->Output("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
}

void YCTask::TaskRequestUpdate(TS_TASK_STATE ts, TaskReqInfo& task_req_info) throw(base::Exception)
{
	switch ( ts )
	{
	case TSTS_Start:							// 任务开始
		task_req_info.status      = m_stateTaskBeg;
		task_req_info.status_desc = "准备采集";
		task_req_info.desc        = "接收到任务，准备开始采集";
		task_req_info.finishtime.clear();
		break;
	case TSTS_EtlException:						// 采集异常
		task_req_info.status      = m_stateEtlException;
		task_req_info.status_desc = "采集异常";
		task_req_info.finishtime  = base::SimpleTime::Now().Time14();
		break;
	case TSTS_AnalyseBegin:						// 开始分析
		task_req_info.status      = m_stateAnaBeg;
		task_req_info.status_desc = "准备分析";
		task_req_info.desc        = "采集成功，准备开始分析";
		task_req_info.finishtime.clear();
		break;
	case TSTS_AnalyseException:					// 分析异常
		task_req_info.status      = m_stateAnaException;
		task_req_info.status_desc = "分析异常";
		task_req_info.finishtime  = base::SimpleTime::Now().Time14();
		break;
	case TSTS_End:								// 任务完成
		task_req_info.status      = m_stateTaskEnd;
		task_req_info.status_desc = "稽核完成";
		task_req_info.desc        = "任务结束";
		task_req_info.finishtime  = base::SimpleTime::Now().Time14();
		break;
	case TSTS_Unknown:							// 未知状态
	default:
		throw base::Exception(TERROR_UPD_TASK_REQ, "Unknown tasksche task state: %d [FILE:%s, LINE:%d]", ts, __FILE__, __LINE__);
	}

	m_pLog->Output("[YC_TASK] Updating task request : %s (%s)", task_req_info.status.c_str(), task_req_info.status_desc.c_str());
	m_pTaskDB->UpdateTaskRequest(task_req_info);
}

void YCTask::HandleEtlTask() throw(base::Exception)
{
	if ( m_vecEtlTaskInfo.empty() )
	{
		return;
	}

	TaskState t_state;
	std::vector<TaskInfo> vEtlTaskInfo;

	const int VEC_ETL_SIZE = m_vecEtlTaskInfo.size();
	for ( int i = 0; i < VEC_ETL_SIZE; ++i )
	{
		TaskInfo& ref_ti = m_vecEtlTaskInfo[i];
		t_state.seq_id = ref_ti.seq_id;
		m_pTaskDB->SelectTaskState(t_state);

		if ( m_etlStateEnd == t_state.state )			// 采集完成
		{
			m_pLog->Output("[YC_TASK] Acquire finished: TASK_ID=%lld, KPI_ID=%s, ETL_ID=%s, ETL_TIME=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str(), ref_ti.etl_time.c_str());

			TaskReqInfo& ref_tri = m_mTaskReqInfo[t_state.seq_id];
			TaskRequestUpdate(TSTS_AnalyseBegin, ref_tri);

			ref_ti.t_type   = TaskInfo::TT_Analyse;
			ref_ti.task_id  = GenerateTaskID();
			ref_ti.kpi_id   = ref_tri.kpi_id;
			//ref_ti.etl_time = EtlTimeTransform(ref_tri.stat_cycle);
			if ( !GetSubRuleID(ref_tri.kpi_id, TaskInfo::TT_Analyse, ref_ti.sub_id) )
			{
				throw base::Exception(TERROR_HDL_ETL_TASK, "Can not find the analyse rule ID! (KPI_ID:%s) [FILE:%s, LINE:%d]", ref_tri.kpi_id.c_str(), __FILE__, __LINE__);
			}
			m_pLog->Output("[YC_TASK] Analyse: TASK_ID=%ld, KPI_ID=%s, ANA_ID=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str());

			// 开始分析任务
			CreateTask(ref_ti);

			m_vecAnaTaskInfo.push_back(ref_ti);
		}
		else if ( m_etlStateError == t_state.state )	// 采集异常
		{
			m_pLog->Output("[YC_TASK] Acquire failed: TASK_ID=%lld, KPI_ID=%s, ETL_ID=%s, ETL_TIME=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str(), ref_ti.etl_time.c_str());

			TaskReqInfo& ref_tri = m_mTaskReqInfo[t_state.seq_id];
			ref_tri.desc = t_state.task_desc;
			m_pLog->Output("[YC_TASK] Acquire exception: %s", ref_tri.desc.c_str());

			TaskRequestUpdate(TSTS_EtlException, ref_tri);
			m_pLog->Output("[YC_TASK] Error task: SEQ=[%d], KPI=[%s], CYCLE=[%s], END_TIME=[%s]", ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str(), ref_tri.finishtime.c_str());

			// 任务删除
			m_mTaskReqInfo.erase(t_state.seq_id);
		}
		else		// 其他状态
		{
			// 检查采集进程是否异常退出
			if ( IsProcessAlive(ref_ti.task_id) )		// 进程是活动的
			{
				vEtlTaskInfo.push_back(ref_ti);
			}
			else	// 进程异常退出
			{
				m_pLog->Output("[YC_TASK] Acquire exited unexpectedly: TASK_ID=%lld, KPI_ID=%s, ETL_ID=%s, ETL_TIME=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str(), ref_ti.etl_time.c_str());

				TaskReqInfo& ref_tri = m_mTaskReqInfo[t_state.seq_id];
				ref_tri.desc = "Process exited unexpectedly!";

				TaskRequestUpdate(TSTS_EtlException, ref_tri);
				m_pLog->Output("[YC_TASK] Error task: SEQ=[%d], KPI=[%s], CYCLE=[%s], END_TIME=[%s]", ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str(), ref_tri.finishtime.c_str());

				// 任务删除
				m_mTaskReqInfo.erase(t_state.seq_id);
			}
		}
	}

	vEtlTaskInfo.swap(m_vecEtlTaskInfo);
}

void YCTask::HandleAnaTask() throw(base::Exception)
{
	if ( m_vecAnaTaskInfo.empty() )
	{
		return;
	}

	TaskState t_state;
	std::vector<TaskInfo> vAnaTaskInfo;

	const int VEC_ANA_SIZE = m_vecAnaTaskInfo.size();
	for ( int i = 0; i < VEC_ANA_SIZE; ++i )
	{
		TaskInfo& ref_ti = m_vecAnaTaskInfo[i];
		t_state.seq_id = ref_ti.seq_id;
		m_pTaskDB->SelectTaskState(t_state);

		if ( m_anaStateEnd == t_state.state )			// 分析完成
		{
			m_pLog->Output("[YC_TASK] Analyse finished: TASK_ID=%lld, KPI_ID=%s, ANA_ID=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str());

			// 移到已完成的任务列表
			TaskReqInfo& ref_tri = m_mTaskReqInfo[t_state.seq_id];
			m_vecEndTask.push_back(ref_tri);
			m_mTaskReqInfo.erase(t_state.seq_id);
		}
		else if ( m_anaStateError == t_state.state )	// 分析异常
		{
			m_pLog->Output("[YC_TASK] Analyse failed: TASK_ID=%lld, KPI_ID=%s, ANA_ID=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str());

			TaskReqInfo& ref_tri = m_mTaskReqInfo[t_state.seq_id];
			ref_tri.desc = t_state.task_desc;
			m_pLog->Output("[YC_TASK] Analyse exception: %s", ref_tri.desc.c_str());

			TaskRequestUpdate(TSTS_AnalyseException, ref_tri);
			m_pLog->Output("[YC_TASK] Error task: SEQ=[%d], KPI=[%s], CYCLE=[%s], END_TIME=[%s]", ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str(), ref_tri.finishtime.c_str());

			// 任务删除
			m_mTaskReqInfo.erase(t_state.seq_id);
		}
		else	// 其他状态
		{
			// 检查分析进程是否异常退出
			if ( IsProcessAlive(ref_ti.task_id) )		// 进程是活动的
			{
				vAnaTaskInfo.push_back(ref_ti);
			}
			else	// 进程异常退出
			{
				m_pLog->Output("[YC_TASK] Analyse exited unexpectedly: TASK_ID=%lld, KPI_ID=%s, ANA_ID=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str());

				TaskReqInfo& ref_tri = m_mTaskReqInfo[t_state.seq_id];
				ref_tri.desc = "Process exited unexpectedly!";

				TaskRequestUpdate(TSTS_AnalyseException, ref_tri);
				m_pLog->Output("[YC_TASK] Error task: SEQ=[%d], KPI=[%s], CYCLE=[%s], END_TIME=[%s]", ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str(), ref_tri.finishtime.c_str());

				// 任务删除
				m_mTaskReqInfo.erase(t_state.seq_id);
			}
		}
	}

	vAnaTaskInfo.swap(m_vecAnaTaskInfo);
}

void YCTask::CreateTask(const TaskInfo& t_info) throw(base::Exception)
{
	std::string command;
	if ( TaskInfo::TT_Acquire == t_info.t_type )		// 采集
	{
		// 更新采集时间
		m_pTaskDB->UpdateEtlTime(t_info.sub_id, t_info.etl_time);

		// 采集程序：守护进程
		base::PubStr::SetFormatString(command, "%s 1 %lld %s %s %s 00001:%s:%s:%d:", m_binAcquire.c_str(), t_info.task_id, m_modeAcquire.c_str(), m_binVer.c_str(), m_cfgAcquire.c_str(), t_info.kpi_id.c_str(), t_info.sub_id.c_str(), t_info.seq_id);
	}
	else if ( TaskInfo::TT_Analyse == t_info.t_type )	// 分析
	{
		// 分析程序：守护进程
		base::PubStr::SetFormatString(command, "%s 1 %lld %s %s %s 00001:%s:%s:%d:", m_binAnalyse.c_str(), t_info.task_id, m_modeAnalyse.c_str(), m_binVer.c_str(), m_cfgAnalyse.c_str(), t_info.kpi_id.c_str(), t_info.sub_id.c_str(), t_info.seq_id);
	}
	else	// 未知
	{
		throw base::Exception(TERROR_CREATE_TASK, "Unknown task type: %d [FILE:%s, LINE:%d]", t_info.t_type, __FILE__, __LINE__);
	}

	// 拉起进程
	m_pLog->Output("[YC_TASK] Execute: %s", command.c_str());
	system(command.c_str());
}

void YCTask::BuildNewTask() throw(base::Exception)
{
	TaskInfo task_info;

	// 新建采集任务
	const int VEC_NEW_TASK_SIZE = m_vecNewTask.size();
	for ( int i = 0; i < VEC_NEW_TASK_SIZE; ++i )
	{
		TaskReqInfo& ref_tri = m_vecNewTask[i];
		m_pLog->Output("[YC_TASK] Get task %d: SEQ=[%d], KPI=[%s], CYCLE=[%s]", (i+1), ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str());

		// 同一流水号的任务重复下发，则认为是重跑数据。
		// 所以摒弃旧任务，重新开始新任务
		if ( m_mTaskReqInfo.find(ref_tri.seq_id) != m_mTaskReqInfo.end() )
		{
			m_pLog->Output("<WARNING> [YC_TASK] The task (SEQ:%d) already exists ! So drop the OLD one !", ref_tri.seq_id);
			RemoveOldTask(ref_tri.seq_id);
		}

		task_info.seq_id   = ref_tri.seq_id;
		task_info.t_type   = TaskInfo::TT_Acquire;
		task_info.task_id  = GenerateTaskID();
		task_info.kpi_id   = ref_tri.kpi_id;
		task_info.etl_time = EtlTimeTransform(ref_tri.stat_cycle);
		GetSubRuleID(ref_tri.kpi_id, TaskInfo::TT_Acquire, task_info.sub_id);
		m_pLog->Output("[YC_TASK] Acquire: TASK_ID=%ld, KPI_ID=%s, ETL_ID=%s, ETL_TIME=%s", task_info.task_id, task_info.kpi_id.c_str(), task_info.sub_id.c_str(), task_info.etl_time.c_str());

		// 开始采集任务
		CreateTask(task_info);

		m_vecEtlTaskInfo.push_back(task_info);
		m_mTaskReqInfo[ref_tri.seq_id] = ref_tri;
	}
}

void YCTask::RemoveOldTask(int task_seq)
{
	// 从正在执行的任务列表删除旧任务
	m_mTaskReqInfo.erase(task_seq);

	// 从已完成的任务列表删除旧任务
	int vec_size = m_vecEndTask.size();
	for ( int i = 0; i < vec_size; ++i )
	{
		if ( task_seq == m_vecEndTask[i].seq_id )
		{
			m_vecEndTask.erase(m_vecEndTask.begin()+i);
			break;
		}
	}

	// 从采集任务列表删除旧任务
	vec_size = m_vecEtlTaskInfo.size();
	for ( int j = 0; j < vec_size; ++j )
	{
		if ( task_seq == m_vecEtlTaskInfo[j].seq_id )
		{
			m_vecEtlTaskInfo.erase(m_vecEtlTaskInfo.begin()+j);
			break;
		}
	}

	// 从分析任务列表删除旧任务
	vec_size = m_vecAnaTaskInfo.size();
	for ( int k = 0; k < vec_size; ++k )
	{
		if ( task_seq == m_vecAnaTaskInfo[k].seq_id )
		{
			m_vecAnaTaskInfo.erase(m_vecAnaTaskInfo.begin()+k);
			break;
		}
	}
}

bool YCTask::GetSubRuleID(const std::string& kpi_id, TaskInfo::TASK_TYPE t_type, std::string& sub_id)
{
	if ( TaskInfo::TT_Acquire == t_type )		// 采集
	{
		// 更新指标规则信息
		KpiRuleInfo kpi_info;
		m_pTaskDB->SelectKpiRule(kpi_id, kpi_info);
		m_mKpiRuleInfo[kpi_id] = kpi_info;

		sub_id = kpi_info.etl_id;
		return true;
	}
	else if ( TaskInfo::TT_Analyse == t_type )	// 分析
	{
		std::map<std::string, KpiRuleInfo>::iterator m_it = m_mKpiRuleInfo.find(kpi_id);
		if ( m_it != m_mKpiRuleInfo.end() )
		{
			sub_id = m_it->second.ana_id;
			return true;
		}
		else	// 没有对应的指标信息
		{
			return false;
		}
	}
	else	// 未知
	{
		return false;
	}
}

void YCTask::FinishTask() throw(base::Exception)
{
	const int VEC_END_TASK_SIZE = m_vecEndTask.size();
	for ( int i = 0; i < VEC_END_TASK_SIZE; ++i )
	{
		TaskReqInfo& ref_tri = m_vecEndTask[i];

		TaskRequestUpdate(TSTS_End, ref_tri);
		m_pLog->Output("[YC_TASK] Finish task: SEQ=[%d], KPI=[%s], CYCLE=[%s], END_TIME=[%s]", ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str(), ref_tri.finishtime.c_str());
	}

	// 清空已完成任务列表
	std::vector<TaskReqInfo>().swap(m_vecEndTask);
}

