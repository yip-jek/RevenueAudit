#include "acquire_hd.h"
#include "log.h"
#include "pubstr.h"

const char* const Acquire_HD::S_HD_ETLRULE_TYPE = "HDJH";			// 话单稽核-采集规则类型

Acquire_HD::Acquire_HD()
{
	m_sType = "话单稽核";
}

Acquire_HD::~Acquire_HD()
{
}

void Acquire_HD::CheckTaskInfo() throw(base::Exception)
{
	if ( m_taskInfo.EtlRuleTime.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "采集时间（周期）为空! 无效! [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	const std::string ETL_TYPE = base::PubStr::TrimUpperB(m_taskInfo.EtlRuleType);
	if ( ETL_TYPE != S_HD_ETLRULE_TYPE )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "不支持的话单稽核采集处理类型: %s [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", ETL_TYPE.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	// 无需检查采集目标表：m_taskInfo.EtlRuleTarget
	// 无需检查采集数据源：m_taskInfo.vecEtlRuleDataSrc
	// 无需检查采集维度  ：m_taskInfo.vecEtlRuleDim
	// 无需检查采集值    ：m_taskInfo.vecEtlRuleVal

	if ( m_taskInfo.EtlCondType != AcqTaskInfo::ETLCTYPE_HDJH_HIVE_SQLS )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "不支持的话单稽核采集条件类型: %d [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.EtlCondType, m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	if ( base::PubStr::TrimB(m_taskInfo.EtlCondition).empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "话单稽核采集条件没有配置！[KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Acquire_HD] 采集类型：[%s] (%s)", m_sType.c_str(), (m_isTest ? "测试":"发布"));
}

