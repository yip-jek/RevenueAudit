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

void Acquire_HD::DoDataAcquisition() throw(base::Exception)
{
	if ( m_taskInfo.DataSrcType != AcqTaskInfo::DSTYPE_HIVE )
	{
		throw base::Exception(ACQERR_DATA_ACQ_FAILED, "不支持的话单稽核数据源类型: %d [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.DataSrcType, m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	Acquire::DoDataAcquisition();
}

int Acquire_HD::RebuildHiveTable() throw(base::Exception)
{
	// 话单稽核 HIVE 目标表无需重建！
	// DO Nothing!
	return 0;
}

void Acquire_HD::CheckSourceTable(bool hive) throw(base::Exception)
{
	m_pLog->Output("[Acquire_HD] Check source table whether exists or not ?");

	if ( m_taskInfo.vecEtlRuleDataSrc.empty() )		// 无配置源表
	{
		m_pLog->Output("[Acquire_HD] NO source table to be checked !");
	}
	else
	{
		const int SRC_TAB_SIZE = m_taskInfo.vecEtlRuleDataSrc.size();

		std::string trans_tab;
		if ( hive )		// HIVE
		{
			for ( int i = 0; i < SRC_TAB_SIZE; ++i )
			{
				trans_tab = TransSourceDate(m_taskInfo.vecEtlRuleDataSrc[i].srcTabName);

				// 检查源表是否存在？
				if ( !m_pAcqHive->CheckTableExisted(trans_tab) ) 	// 表不存在
				{
					throw base::Exception(ACQERR_CHECK_SRC_TAB_FAILED, "[HIVE] Source table do not exist: %s (KPI_ID:%s, ETL_ID:%s) [FILE:%s, LINE:%d]", trans_tab.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
				}
			}
		}
		else	// DB2
		{
			for ( int i = 0; i < SRC_TAB_SIZE; ++i )
			{
				trans_tab = TransSourceDate(m_taskInfo.vecEtlRuleDataSrc[i].srcTabName);

				// 检查源表是否存在？
				if ( !m_pAcqDB2->CheckTableExisted(trans_tab) )		// 表不存在
				{
					throw base::Exception(ACQERR_CHECK_SRC_TAB_FAILED, "[DB2] Source table do not exist: %s (KPI_ID:%s, ETL_ID:%s) [FILE:%s, LINE:%d]", trans_tab.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
				}
			}
		}

		m_pLog->Output("[Acquire_HD] Check source table OK.");
	}
}

void Acquire_HD::TaskInfo2Sql(std::vector<std::string>& vec_sql, bool hive) throw(base::Exception)
{
	// 不支持从 DB2 采集源数据
	if ( !hive )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "[HDJH] Not support to acquire source data from DB2 ! (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	// 支付多个 SQL 语句，以分号（';'）分隔
	std::vector<std::string> vec_cond_sql;
	base::PubStr::Str2StrVector(m_taskInfo.EtlCondition, ";", vec_cond_sql);

	int vec_sql_size = vec_cond_sql.size();
	for ( int i = 0; i < VEC_SQL_SIZE; ++i )
	{
		std::string& ref_str = vec_cond_sql[i];
		if ( ref_str.empty() )		// 去除空 SQL 语句
		{
			vec_cond_sql.erase(vec_cond_sql.begin()+i);
			--i;
			--vec_sql_size;
		}
		else	// 进行标志转换
		{
			ExchangeSQLMark(ref_str);
		}
	}

	if ( vec_cond_sql.empty() )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "[HDJH] Not effective hive sql(s) ! (ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	vec_cond_sql.swap(vec_sql);
}

