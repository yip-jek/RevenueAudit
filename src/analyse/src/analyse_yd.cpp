#include "analyse_yd.h"
#include "pubstr.h"
#include "simpletime.h"
#include "log.h"
#include "canadb2.h"

Analyse_YD::Analyse_YD()
:m_taskScheLogID(0)
{
	m_sType = "一点稽核";
}

Analyse_YD::~Analyse_YD()
{
}

std::string Analyse_YD::GetLogFilePrefix()
{
	return std::string("Analyse_YD");
}

void Analyse_YD::LoadConfig() throw(base::Exception)
{
	Analyse::LoadConfig();

	m_cfg.RegisterItem("TABLE", "TAB_TASK_SCHE_LOG");		// 读取任务日程日志表配置
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_REQUEST");

	m_cfg.ReadConfig();

	m_tabTaskScheLog  = m_cfg.GetCfgValue("TABLE", "TAB_TASK_SCHE_LOG");
	m_tabAlarmRequest = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_REQUEST");

	m_pLog->Output("[Analyse_YD] Load configuration OK.");
}

void Analyse_YD::Init() throw(base::Exception)
{
	Analyse::Init();

	m_pAnaDB2->SetTabTaskScheLog(m_tabTaskScheLog);
	m_pAnaDB2->SetTabAlarmRequest(m_tabAlarmRequest);

	m_pLog->Output("[Analyse_YD] Init OK.");
}

void Analyse_YD::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	// 更新任务日程日志状态
	if ( 0 == err_code )	// 分析成功
	{
		// 只有分析成功才生成告警请求
		AlarmRequest();

		// 更新任务日志状态为："ANA_SUCCEED"（分析成功）
		m_pAnaDB2->UpdateTaskScheLogState(m_taskScheLogID, base::SimpleTime::Now().Time14(), "ANA_SUCCEED", "分析成功完成", "");
	}
	else	// 分析失败
	{
		// 更新任务日志状态为："ANA_FAILED"（分析失败）
		std::string str_error;
		base::PubStr::SetFormatString(str_error, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
		m_pAnaDB2->UpdateTaskScheLogState(m_taskScheLogID, base::SimpleTime::Now().Time14(), "ANA_FAILED", "分析失败", str_error);
	}

	Analyse::End(err_code, err_msg);
}

void Analyse_YD::GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < 4 )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法拆分出任务日程日志ID! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	std::string& ref_str = vec_str[3];
	if ( !base::PubStr::Str2Int(ref_str, m_taskScheLogID) )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "无效的任务日程日志ID：%s [FILE:%s, LINE:%d]", ref_str.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Analyse_YD] 任务日程日志ID：%d", m_taskScheLogID);
}

void Analyse_YD::AlarmRequest()
{
	// >>>>> 告警标记 <<<<<
	// ALARM_REQ: 告警请求
	std::string alarm_flag = base::PubStr::TrimUpperB(m_taskInfo.AlarmID);
	if ( "ALARM_REQ" == alarm_flag )
	{
	}
}

