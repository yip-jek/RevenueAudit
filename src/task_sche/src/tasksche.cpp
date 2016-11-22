#include "tasksche.h"
#include <errno.h>
#include "config.h"
#include "log.h"
#include "pubstr.h"
#include "pubtime.h"
#include "simpletime.h"
#include "taskdb2.h"


TaskSche::TaskSche(base::Config& cfg)
:m_pCfg(&cfg)
,m_pLog(base::Log::Instance())
,m_waitSeconds(0)
,m_noTaskTime(0)
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
	return ("TaskSche: Version 1.0003 released. Compiled at "__TIME__" on "__DATE__);
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
	// 随机数种子
	srand(time(0));

	LoadConfig();

	InitConnect();

	Check();

	m_pLog->Output("[TASK] Init OK.");
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

	m_pCfg->RegisterItem("STATE", "ETL_END_STATE");
	m_pCfg->RegisterItem("STATE", "ETL_ERROR_STATE");
	m_pCfg->RegisterItem("STATE", "ANA_END_STATE");
	m_pCfg->RegisterItem("STATE", "ANA_ERROR_STATE");

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

	m_etlStateEnd   = m_pCfg->GetCfgValue("STATE", "ETL_END_STATE");
	m_etlStateError = m_pCfg->GetCfgValue("STATE", "ETL_ERROR_STATE");
	m_anaStateEnd   = m_pCfg->GetCfgValue("STATE", "ANA_END_STATE");
	m_anaStateError = m_pCfg->GetCfgValue("STATE", "ANA_ERROR_STATE");

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

	// 指定工作目录为Hive代理的路径
	if ( chdir(m_hiveAgentPath.c_str()) < 0 )
	{
		throw base::Exception(TERROR_CHECK, "Change work dir to '%s' failed! %s [FILE:%s, LINE:%d]", m_hiveAgentPath.c_str(), strerror(errno), __FILE__, __LINE__);
	}
	m_pLog->Output("[TASK] Change work dir to '%s' OK.", m_hiveAgentPath.c_str());

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

		ExecuteTask();

		FinishTask();
	}
}

void TaskSche::GetNewTask()
{
	m_pTaskDB->SelectNewTaskRequest(m_vecNewTask);

	if ( m_vecNewTask.empty() )
	{
		// 达到 NO_TASK_MAX_TIME（秒）都无新任务，则日志输出
		if ( m_noTaskTime > 0 )
		{
			if ( (time(NULL) - m_noTaskTime) >= NO_TASK_MAX_TIME )
			{
				m_noTaskTime = time(NULL);

				if ( m_mTaskReqInfo.empty() )
				{
					m_pLog->Output("[TASK] NO task.");
				}
				else
				{
					m_pLog->Output("[TASK] Running task size: %lu", m_mTaskReqInfo.size());
				}
			}
		}
		else
		{
			m_noTaskTime = time(NULL);
		}
	}
	else
	{
		m_noTaskTime = 0;

		// 更新任务状态
		const int VEC_NEW_TASK_SIZE = m_vecNewTask.size();
		for ( int i = 0; i < VEC_NEW_TASK_SIZE; ++i )
		{
			TaskRequestUpdate(TSTS_Start, m_vecNewTask[i]);
		}

		m_pLog->Output("[TASK] Get the new task size: %d", VEC_NEW_TASK_SIZE);
	}
}

void TaskSche::ExecuteTask()
{
	HandleAnaTask();

	HandleEtlTask();

	BuildNewTask();
}

void TaskSche::TaskRequestUpdate(TS_TASK_STATE ts, TaskReqInfo& task_req_info) throw(base::Exception)
{
	switch ( ts )
	{
	case TSTS_Start:							// 任务开始
		task_req_info.status      = "10";
		task_req_info.status_desc = "准备采集";
		task_req_info.desc        = "接收到任务，准备开始采集";
		task_req_info.finishtime.clear();
		break;
	case TSTS_EtlException:						// 采集异常
		task_req_info.status      = "02";
		task_req_info.status_desc = "采集异常";
		task_req_info.finishtime  = base::SimpleTime::Now().Time14();
		break;
	case TSTS_AnalyseBegin:						// 开始分析
		task_req_info.status      = "20";
		task_req_info.status_desc = "准备分析";
		task_req_info.desc        = "采集成功，准备开始分析";
		task_req_info.finishtime.clear();
		break;
	case TSTS_AnalyseException:					// 分析异常
		task_req_info.status      = "03";
		task_req_info.status_desc = "分析异常";
		task_req_info.finishtime  = base::SimpleTime::Now().Time14();
		break;
	case TSTS_End:								// 任务完成
		task_req_info.status      = "01";
		task_req_info.status_desc = "稽核完成";
		task_req_info.desc        = "任务结束";
		task_req_info.finishtime  = base::SimpleTime::Now().Time14();
		break;
	case TSTS_Unknown:							// 未知状态
	default:
		throw base::Exception(TERROR_UPD_TASK_REQ, "Unknown tasksche task state: %d [FILE:%s, LINE:%d]", ts, __FILE__, __LINE__);
	}

	m_pLog->Output("[TASK] Updating task request : %s (%s)", task_req_info.status.c_str(), task_req_info.status_desc.c_str());
	m_pTaskDB->UpdateTaskRequest(task_req_info);
}

void TaskSche::HandleEtlTask()
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
			m_pLog->Output("[TASK] Acquire finished: TASK_ID=%lld, KPI_ID=%s, ETL_ID=%s, ETL_TIME=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str(), ref_ti.etl_time.c_str());

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
			m_pLog->Output("[TASK] Analyse: TASK_ID=%ld, KPI_ID=%s, ANA_ID=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str());

			// 开始分析任务
			CreateTask(ref_ti);

			m_vecAnaTaskInfo.push_back(ref_ti);
		}
		else if ( m_etlStateError == t_state.state )	// 采集异常
		{
			m_pLog->Output("[TASK] Acquire failed: TASK_ID=%lld, KPI_ID=%s, ETL_ID=%s, ETL_TIME=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str(), ref_ti.etl_time.c_str());

			TaskReqInfo& ref_tri = m_mTaskReqInfo[t_state.seq_id];
			ref_tri.desc = t_state.task_desc;
			m_pLog->Output("[TASK] Acquire exception: %s", ref_tri.desc.c_str());

			TaskRequestUpdate(TSTS_EtlException, ref_tri);
			m_pLog->Output("[TASK] Error task: SEQ=[%d], KPI=[%s], CYCLE=[%s], END_TIME=[%s]", ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str(), ref_tri.finishtime.c_str());

			// 任务删除
			m_mTaskReqInfo.erase(t_state.seq_id);
		}
		else		// 其他状态
		{
			vEtlTaskInfo.push_back(ref_ti);
		}
	}

	vEtlTaskInfo.swap(m_vecEtlTaskInfo);
}

void TaskSche::HandleAnaTask()
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
			m_pLog->Output("[TASK] Analyse finished: TASK_ID=%lld, KPI_ID=%s, ETL_ID=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str());

			// 移到已完成的任务列表
			TaskReqInfo& ref_tri = m_mTaskReqInfo[t_state.seq_id];
			m_vecEndTask.push_back(ref_tri);
			m_mTaskReqInfo.erase(t_state.seq_id);
		}
		else if ( m_anaStateError == t_state.state )	// 分析异常
		{
			m_pLog->Output("[TASK] Analyse failed: TASK_ID=%lld, KPI_ID=%s, ETL_ID=%s", ref_ti.task_id, ref_ti.kpi_id.c_str(), ref_ti.sub_id.c_str());

			TaskReqInfo& ref_tri = m_mTaskReqInfo[t_state.seq_id];
			ref_tri.desc = t_state.task_desc;
			m_pLog->Output("[TASK] Analyse exception: %s", ref_tri.desc.c_str());

			TaskRequestUpdate(TSTS_AnalyseException, ref_tri);
			m_pLog->Output("[TASK] Error task: SEQ=[%d], KPI=[%s], CYCLE=[%s], END_TIME=[%s]", ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str(), ref_tri.finishtime.c_str());

			// 任务删除
			m_mTaskReqInfo.erase(t_state.seq_id);
		}
		else		// 其他状态
		{
			vAnaTaskInfo.push_back(ref_ti);
		}
	}

	vAnaTaskInfo.swap(m_vecAnaTaskInfo);
}

void TaskSche::CreateTask(const TaskInfo& t_info) throw(base::Exception)
{
	std::string command;
	if ( TaskInfo::TT_Acquire == t_info.t_type )		// 采集
	{
		// 更新采集时间
		m_pTaskDB->UpdateEtlTime(t_info.sub_id, t_info.etl_time);

		// 采集程序：守护进程
		base::PubStr::SetFormatString(command, "%s 1 %lld %s 00001:%s:%s:%d:", m_binAcquire.c_str(), t_info.task_id, m_cfgAcquire.c_str(), t_info.kpi_id.c_str(), t_info.sub_id.c_str(), t_info.seq_id);
	}
	else if ( TaskInfo::TT_Analyse == t_info.t_type )	// 分析
	{
		// 分析程序：守护进程
		base::PubStr::SetFormatString(command, "%s 1 %lld %s 00001:%s:%s:%d:", m_binAnalyse.c_str(), t_info.task_id, m_cfgAnalyse.c_str(), t_info.kpi_id.c_str(), t_info.sub_id.c_str(), t_info.seq_id);
	}
	else	// 未知
	{
		throw base::Exception(TERROR_CREATE_TASK, "Unknown task type: %d [FILE:%s, LINE:%d]", t_info.t_type, __FILE__, __LINE__);
	}

	// 拉起进程
	m_pLog->Output("[TASK] Execute: %s", command.c_str());
	system(command.c_str());
}

void TaskSche::BuildNewTask()
{
	TaskInfo task_info;

	// 新建采集任务
	const int VEC_NEW_TASK_SIZE = m_vecNewTask.size();
	for ( int i = 0; i < VEC_NEW_TASK_SIZE; ++i )
	{
		TaskReqInfo& ref_tri = m_vecNewTask[i];
		m_pLog->Output("[TASK] Get task %d: SEQ=[%d], KPI=[%s], CYCLE=[%s]", (i+1), ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str());

		// 同一流水号的任务重复下发，则认为是重跑数据。
		// 所以摒弃旧任务，重新开始新任务
		if ( m_mTaskReqInfo.find(ref_tri.seq_id) != m_mTaskReqInfo.end() )
		{
			m_pLog->Output("<WARNING> [TASK] The task (SEQ:%d) already exists ! So drop the OLD one !", ref_tri.seq_id);
			RemoveOldTask(ref_tri.seq_id);
		}

		task_info.seq_id   = ref_tri.seq_id;
		task_info.t_type   = TaskInfo::TT_Acquire;
		task_info.task_id  = GenerateTaskID();
		task_info.kpi_id   = ref_tri.kpi_id;
		task_info.etl_time = EtlTimeTransform(ref_tri.stat_cycle);
		GetSubRuleID(ref_tri.kpi_id, TaskInfo::TT_Acquire, task_info.sub_id);
		m_pLog->Output("[TASK] Acquire: TASK_ID=%ld, KPI_ID=%s, ETL_ID=%s, ETL_TIME=%s", task_info.task_id, task_info.kpi_id.c_str(), task_info.sub_id.c_str(), task_info.etl_time.c_str());

		// 开始采集任务
		CreateTask(task_info);

		m_vecEtlTaskInfo.push_back(task_info);
		m_mTaskReqInfo[ref_tri.seq_id] = ref_tri;
	}
}

void TaskSche::RemoveOldTask(int task_seq)
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

long long TaskSche::GenerateTaskID()
{
	// 取14位时间（格式：YYYYMMDDHHMISS）的后12位
	std::string time_str = base::SimpleTime::Now().Time14().substr(2);

	long long new_taskid = 0;
	base::PubStr::Str2LLong(time_str, new_taskid);

	// 加上随机数
	new_taskid *= 1000;
	new_taskid += (rand() % 1000);
	return new_taskid;
}

bool TaskSche::GetSubRuleID(const std::string& kpi_id, TaskInfo::TASK_TYPE t_type, std::string& sub_id)
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

std::string TaskSche::EtlTimeTransform(const std::string& cycle) throw(base::Exception)
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

void TaskSche::FinishTask()
{
	const int VEC_END_TASK_SIZE = m_vecEndTask.size();
	for ( int i = 0; i < VEC_END_TASK_SIZE; ++i )
	{
		TaskReqInfo& ref_tri = m_vecEndTask[i];

		TaskRequestUpdate(TSTS_End, ref_tri);
		m_pLog->Output("[TASK] Finish task: SEQ=[%d], KPI=[%s], CYCLE=[%s], END_TIME=[%s]", ref_tri.seq_id, ref_tri.kpi_id.c_str(), ref_tri.stat_cycle.c_str(), ref_tri.finishtime.c_str());
	}

	// 清空已完成任务列表
	std::vector<TaskReqInfo>().swap(m_vecEndTask);

	// 等待下一次的任务执行
	sleep(m_waitSeconds);
}

