#include "acquire_yc.h"
#include "pubstr.h"


Acquire_YC::Acquire_YC()
:m_ycSeqID(0)
{
}

Acquire_YC::~Acquire_YC()
{
}

void Acquire_YC::LoadConfig() throw(base::Exception)
{
	Acquire::LoadConfig();

	m_cfg.RegisterItem("TABLE", "TAB_YC_TASK_REQ");

	m_cfg.ReadConfig();

	m_tabYCTaskReq = m_cfg.GetCfgValue("TABLE", "TAB_YC_TASK_REQ");

	m_pLog->Output("[Acquire_YC] Load configuration OK.");
}

void Acquire_YC::Init() throw(base::Exception)
{
	Acquire::Init();

	m_pAcqDB2->SetTabYCTaskReq(m_tabYCTaskReq);

	// 更新任务状态为；"11"（正在采集）
	m_pAcqDB2->UpdateYCTaskReq(m_ycSeqID, "11", "正在采集", "采集开始时间："+base::SimpleTime::Now().TimeStamp());

	m_pLog->Output("[Acquire_YC] Init OK.");
}

void Acquire_YC::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	// 更新任务状态
	std::string task_desc;
	if ( 0 == err_code )	// 正常退出
	{
		// 更新任务状态为；"12"（采集完成）
		task_desc = "采集结束时间：" + base::SimpleTime::Now().TimeStamp();
		m_pAcqDB2->UpdateYCTaskReq(m_ycSeqID, "12", "采集完成", task_desc);
	}
	else	// 异常退出
	{
		// 更新任务状态为；"13"（采集失败）
		base::PubStr::SetFormatString(task_desc, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
		m_pAcqDB2->UpdateYCTaskReq(m_ycSeqID, "13", "采集失败", task_desc);
	}

	Acquire::End(err_code, err_msg);
}

void Acquire_YC::GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < 4 )
	{
		throw base::Exception(Acquire::ACQERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法拆分出业财任务流水号! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::Str2Int(vec_str[3], m_ycSeqID) )
	{
		throw base::Exception(Acquire::ACQERR_TASKINFO_ERROR, "无效的业财任务流水号：%s [FILE:%s, LINE:%d]", vec_str[3].c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Acquire] 业财稽核任务流水号：%d", m_ycSeqID);
}

