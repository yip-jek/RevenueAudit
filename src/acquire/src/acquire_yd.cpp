#include "acquire_yd.h"
#include "pubstr.h"
#include "simpletime.h"
#include "log.h"
#include "cacqdb2.h"

Acquire_YD::Acquire_YD()
:m_taskScheLogID(0)
{
	m_sType = "一点稽核";
}

Acquire_YD::~Acquire_YD()
{
}

std::string Acquire_YD::GetLogFilePrefix()
{
	return std::string("Acquire_YD");
}

void Acquire_YD::LoadConfig() throw(base::Exception)
{
	Acquire::LoadConfig();

	// 读取任务日程日志表配置
	m_cfg.RegisterItem("TABLE", "TAB_TASK_SCHE_LOG");
	m_cfg.ReadConfig();
	m_tabTaskScheLog = m_cfg.GetCfgValue("TABLE", "TAB_TASK_SCHE_LOG");

	m_pLog->Output("[Acquire_YD] Load configuration OK.");
}

void Acquire_YD::Init() throw(base::Exception)
{
	Acquire::Init();

	m_pAcqDB2->SetTabTaskScheLog(m_tabTaskScheLog);

	m_pLog->Output("[Acquire_YD] Init OK.");
}

void Acquire_YD::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	// 更新任务日程日志状态
	if ( 0 == err_code )	// 采集成功
	{
		// 更新任务日志状态为："ETL_SUCCEED"（采集成功）
		m_pAcqDB2->UpdateTaskScheLogState(m_taskScheLogID, base::SimpleTime::Now().Time14(), "ETL_SUCCEED", "采集成功完成", "");
	}
	else	// 采集失败
	{
		// 更新任务日志状态为："ETL_FAILED"（采集失败）
		std::string str_error;
		base::PubStr::SetFormatString(str_error, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
		m_pAcqDB2->UpdateTaskScheLogState(m_taskScheLogID, base::SimpleTime::Now().Time14(), "ETL_FAILED", "采集失败", str_error);
	}

	Acquire::End(err_code, err_msg);
}

void Acquire_YD::GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < 4 )
	{
		throw base::Exception(ACQERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法拆分出任务日程日志ID! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	std::string& ref_str = vec_str[3];
	if ( !base::PubStr::Str2Int(ref_str, m_taskScheLogID) )
	{
		throw base::Exception(ACQERR_TASKINFO_ERROR, "无效的任务日程日志ID：%s [FILE:%s, LINE:%d]", ref_str.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Acquire_YD] 任务日程日志ID：%d", m_taskScheLogID);
}

