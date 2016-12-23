#include "acquire_yc.h"
#include "pubstr.h"
#include "log.h"
#include "simpletime.h"
#include "cacqdb2.h"
#include "cacqhive.h"

const char* const Acquire_YC::S_YC_ETLRULE_TYPE = "YCRA";			// 业财稽核-采集规则类型

Acquire_YC::Acquire_YC()
:m_ycSeqID(0)
{
	m_sType = "业财稽核";
}

Acquire_YC::~Acquire_YC()
{
}

void Acquire_YC::LoadConfig() throw(base::Exception)
{
	Acquire::LoadConfig();

	m_cfg.RegisterItem("TABLE", "TAB_YC_TASK_REQ");
	m_cfg.RegisterItem("TABLE", "TAB_YCRA_STATRULE");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_PERIOD");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_CITY");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_BATCH");

	m_cfg.ReadConfig();

	m_tabYCTaskReq = m_cfg.GetCfgValue("TABLE", "TAB_YC_TASK_REQ");
	m_tabStatRule  = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_STATRULE");
	m_fieldPeriod  = m_cfg.GetCfgValue("FIELD", "SRC_FIELD_PERIOD");
	m_fieldCity    = m_cfg.GetCfgValue("FIELD", "SRC_FIELD_CITY");
	m_fieldBatch   = m_cfg.GetCfgValue("FIELD", "SRC_FIELD_BATCH");

	m_pLog->Output("[Acquire_YC] Load configuration OK.");
}

std::string Acquire_YC::GetLogFilePrefix()
{
	return std::string("Acquire_YC");
}

void Acquire_YC::Init() throw(base::Exception)
{
	Acquire::Init();

	m_pAcqDB2->SetTabYCTaskReq(m_tabYCTaskReq);
	m_pAcqDB2->SetTabYCStatRule(m_tabStatRule);

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
	m_pLog->Output("[Acquire_YC] 业财稽核任务流水号：%d", m_ycSeqID);
}

void Acquire_YC::FetchTaskInfo() throw(base::Exception)
{
	Acquire::FetchTaskInfo();

	// 获取任务地市信息
	m_pAcqDB2->SelectYCTaskReqCity(m_ycSeqID, m_taskCity);
	base::PubStr::Trim(m_taskCity);
	m_pLog->Output("[Acquire_YC] Get task request city: [%s]", m_taskCity.c_str());

	m_pLog->Output("[Acquire_YC] 获取业财稽核因子规则信息 ...");
	m_pAcqDB2->SelectYCStatRule(m_taskInfo.KpiID, m_vecYCInfo);
}

void Acquire_YC::CheckTaskInfo() throw(base::Exception)
{
	Acquire::CheckTaskInfo();

	const std::string ETL_TYPE = base::PubStr::TrimUpperB(m_taskInfo.EtlRuleType);
	if ( ETL_TYPE != S_YC_ETLRULE_TYPE )
	{
		throw base::Exception(ACQERR_TASKINFO_INVALID, "不支持的业财稽核采集处理类型: %s [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", ETL_TYPE.c_str(), m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}
}

void Acquire_YC::CheckSourceTable(bool hive) throw(base::Exception)
{
	m_pLog->Output("[Acquire_YC] Check source table whether exists or not ?");

	if ( m_taskInfo.vecEtlRuleDataSrc.empty() )		// 无配置源表
	{
		m_pLog->Output("[Acquire_YC] NO source table to be checked !");
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

		m_pLog->Output("[Acquire_YC] Check source table OK.");
	}
}

void Acquire_YC::TaskInfo2Sql(std::vector<std::string>& vec_sql, bool hive) throw(base::Exception)
{
	const int VEC_YCINFO_SIZE = m_vecYCInfo.size();
	if ( VEC_YCINFO_SIZE <= 0 )
	{
		throw base::Exception(ACQERR_YC_STATRULE_SQL_FAILED, "没有业财稽核因子规则信息! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	// HIVE: 前置 insert sql
	std::string prefix_sql;
	if ( hive )
	{
		prefix_sql = "insert " + m_sInsertMode + " table " + m_taskInfo.EtlRuleTarget + " ";
	}

	size_t pos = 0;
	std::string yc_dimid;
	std::string yc_sql;
	std::vector<std::string> v_yc_sql;

	for ( int i = 0; i < VEC_YCINFO_SIZE; ++i )
	{
		YCInfo& ref_ycinf = m_vecYCInfo[i];
		m_pLog->Output("[Acquire_YC] 业财统计因子SQL [%d]: %s", (i+1), ref_ycinf.stat_sql.c_str());

		yc_dimid = base::PubStr::TrimUpperB(ref_ycinf.stat_dimid);
		if ( yc_dimid.empty() )
		{
			throw base::Exception(ACQERR_YC_STATRULE_SQL_FAILED, "无效的业财稽核统计因子维度ID！(KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
		}
		yc_dimid = "'" + yc_dimid + "', ";

		yc_sql = base::PubStr::UpperB(ref_ycinf.stat_sql);
		pos = yc_sql.find("SELECT ");
		if ( std::string::npos == pos )
		{
			throw base::Exception(ACQERR_YC_STATRULE_SQL_FAILED, "业财稽核统计因子SQL没有正确配置！(KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
		}

		pos += 7;
		yc_sql = ref_ycinf.stat_sql;
		yc_sql.insert(pos, yc_dimid);
		yc_sql.insert(0, prefix_sql);

		ExchangeSQLMark(yc_sql);
		ExtendSQLCondition(yc_sql);

		m_pLog->Output("[Acquire_YC] (转换后) 统计因子SQL [%d]: %s", (i+1), yc_sql.c_str());
		v_yc_sql.push_back(yc_sql);
	}

	v_yc_sql.swap(vec_sql);
}

void Acquire_YC::ExtendSQLCondition(std::string& sql) throw(base::Exception)
{
	const std::string EX_SUB_COND;
	const std::string EXTEND_SQL_COND;
	base::PubStr::SetFormatString(const_cast<std::string&>(EX_SUB_COND), "%s = '%s' and %s = '%s'", m_fieldPeriod.c_str(), m_acqDate.c_str(), m_fieldCity.c_str(), m_taskCity.c_str());
	base::PubStr::SetFormatString(const_cast<std::string&>(EXTEND_SQL_COND), "%s and %s in (select max(%s) from ", EX_SUB_COND.c_str(), m_fieldBatch.c_str(), m_fieldBatch.c_str());

	const std::string MARK_FROM  = " FROM ";
	const std::string MARK_WHERE = "WHERE ";
	const size_t M_FROM_SIZE     = MARK_FROM.size();
	const size_t M_WHERE_SIZE    = MARK_WHERE.size();
	const std::string C_SQL = base::PubStr::UpperB(sql);

	size_t f_pos = 0;
	size_t w_pos = 0;
	size_t off   = 0;
	std::string tab_src;
	std::string str_add;

	while ( (f_pos = C_SQL.find(MARK_FROM, f_pos)) != std::string::npos )
	{
		f_pos += M_FROM_SIZE;

		// Skip spaces, find the beginning of table name
		f_pos = C_SQL.find_first_not_of('\x20', f_pos);
		if ( std::string::npos == f_pos )	// All spaces
		{
			throw base::Exception(ACQERR_ADD_CITY_BATCH_FAILED, "业财稽核统计因子SQL无法添加地市和批次：NO table name! [pos:%llu] (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", f_pos, m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
		}

		// Find the end of table name
		w_pos = C_SQL.find_first_of('\x20', f_pos);
		if ( std::string::npos == w_pos )	// No space
		{
			tab_src = C_SQL.substr(f_pos);
			base::PubStr::SetFormatString(str_add, " where %s %s where %s)", EXTEND_SQL_COND.c_str(), tab_src.c_str(), EX_SUB_COND.c_str());
			sql += str_add;
			break;
		}
		else
		{
			tab_src = C_SQL.substr(f_pos, w_pos-f_pos);
			if ( '(' == tab_src[0] )	// Skip: 非表名
			{
				f_pos = w_pos;
				continue;
			}
			base::PubStr::SetFormatString(str_add, "%s %s where %s)", EXTEND_SQL_COND.c_str(), tab_src.c_str(), EX_SUB_COND.c_str());

			w_pos = C_SQL.find_first_not_of('\x20', w_pos);
			if ( std::string::npos == w_pos )	// All spaces
			{
				str_add = " where " + str_add;
				sql += str_add;
				break;
			}
			else if ( C_SQL.substr(w_pos, M_WHERE_SIZE) == MARK_WHERE )	// 表名后带"WHERE"
			{
				str_add += (" and ");
				w_pos += M_WHERE_SIZE;
			}
			else	// 表名后不带"WHERE"
			{
				str_add = " where " + str_add + " ";
			}

			sql.insert(w_pos+off, str_add);
			off += str_add.size();
			f_pos = w_pos;
		}
	}
}

