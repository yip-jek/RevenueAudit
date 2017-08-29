#include "acquire_yc.h"
#include "pubstr.h"
#include "log.h"
#include "simpletime.h"
#include "cacqdb2.h"
#include "cacqhive.h"

const char* const Acquire_YC::S_YC_ETLRULE_TYPE = "YCRA";				// 业财稽核-采集规则类型

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
	m_cfg.RegisterItem("TABLE", "TAB_DICT_CITY");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_PERIOD");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_CITY");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_BATCH");

	m_cfg.ReadConfig();

	m_tabYCTaskReq = m_cfg.GetCfgValue("TABLE", "TAB_YC_TASK_REQ");
	m_tabStatRule  = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_STATRULE");
	m_tabDictCity  = m_cfg.GetCfgValue("TABLE", "TAB_DICT_CITY");
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
	m_pAcqDB2->SetTabYCDictCity(m_tabDictCity);

	// 更新任务状态为："11"（正在采集）
	m_pAcqDB2->UpdateYCTaskReq(m_ycSeqID, "11", "正在采集", "采集开始时间："+base::SimpleTime::Now().TimeStamp());

	m_pLog->Output("[Acquire_YC] Init OK.");
}

void Acquire_YC::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	// 更新任务状态
	std::string task_desc;
	if ( 0 == err_code )	// 正常退出
	{
		// 更新任务状态为："12"（采集完成）
		task_desc = "采集结束时间：" + base::SimpleTime::Now().TimeStamp();
		m_pAcqDB2->UpdateYCTaskReq(m_ycSeqID, "12", "采集完成", task_desc);
	}
	else	// 异常退出
	{
		// 更新任务状态为："13"（采集失败）
		base::PubStr::SetFormatString(task_desc, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
		m_pAcqDB2->UpdateYCTaskReq(m_ycSeqID, "13", "采集失败", task_desc);
	}

	Acquire::End(err_code, err_msg);
}

void Acquire_YC::GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < 4 )
	{
		throw base::Exception(ACQERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法拆分出业财任务流水号! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::Str2Int(vec_str[3], m_ycSeqID) )
	{
		throw base::Exception(ACQERR_TASKINFO_ERROR, "无效的业财任务流水号：%s [FILE:%s, LINE:%d]", vec_str[3].c_str(), __FILE__, __LINE__);
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

	if ( m_pAcqDB2->SelectYCTaskCityCN(m_taskCity, m_taskCityCN) )
	{
		m_pLog->Output("[Acquire_YC] Get task city CN: [%s]", m_taskCityCN.c_str());
	}
	else
	{
		m_pLog->Output("<WARNING> [Acquire_YC] Get task city CN failed !!!");
	}

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

void Acquire_YC::DoDataAcquisition() throw(base::Exception)
{
	m_pLog->Output("[Acquire_YC] 分析业财稽核采集规则 ...");
	GenerateEtlDate(m_taskInfo.EtlRuleTime);

	// 暂时只支持从 DB2 数据库采集源数据
	if ( AcqTaskInfo::DSTYPE_DB2 != m_taskInfo.DataSrcType )
	{
		throw base::Exception(ACQERR_DATA_ACQ_FAILED, "不支持的数据源类型: %d [KPI_ID:%s, ETL_ID:%s] [FILE:%s, LINE:%d]", m_taskInfo.DataSrcType, m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	YCDataAcquisition();
	m_pLog->Output("[Acquire_YC] 采集业财稽核数据完成.");
}

void Acquire_YC::YCDataAcquisition() throw(base::Exception)
{
	// 载入 HDFS 配置
	LoadHdfsConfig();

	// 创建 HDFS 连接
	HdfsConnector* pHdfsConnector = new HdfsConnector(m_sHdfsHost, m_nHdfsPort);
	base::AutoDisconnect a_disconn(pHdfsConnector);		// 资源自动释放
	a_disconn.Connect();

	// 采集结果表字段包含维度ID
	// 但采集 SQL 并没有加入维度ID，维度ID在采集完结果数据后再进行补全
	// 因此此处要减去1
	const int FIELD_SIZE = RebuildHiveTable() - 1;

	// 重建目标表后，再检查源表是否存在
	CheckSourceTable(false);
	HandleYCInfo();

	m_pLog->Output("[Acquire_YC] 执行 DB2 的业财数据采集 ...");
	hdfsFS hd_fs = pHdfsConnector->GetHdfsFS();
	std::vector<std::vector<std::string> > vec2_data;

	const int VEC_YCI_SIZE = m_vecYCInfo.size();
	for ( int i = 0; i < VEC_YCI_SIZE; ++i )
	{
		YCInfo& ref_yci = m_vecYCInfo[i];

		// 从 DB2 数据库中采集结果数据
		m_pAcqDB2->FetchEtlData(ref_yci.stat_sql, FIELD_SIZE, vec2_data);
		MakeYCResultComplete(ref_yci.stat_dimid, FIELD_SIZE, vec2_data);

		// 将采集结果数据写入 HDFS 再 load 到 HIVE
		std::string hdfsFile = GeneralHdfsFileName();
		hdfsFile = DB2DataOutputHdfsFile(vec2_data, hd_fs, hdfsFile);
		LoadHdfsFile2Hive(m_taskInfo.EtlRuleTarget, hdfsFile);
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

void Acquire_YC::GenerateEtlDate(const std::string& date_fmt) throw(base::Exception)
{
	Acquire::GenerateEtlDate(date_fmt);

	// 重设采集 (HIVE) 目标表名：加上地市与账期
	base::PubStr::SetFormatString(m_taskInfo.EtlRuleTarget, "%s_%s_%s", m_taskInfo.EtlRuleTarget.c_str(), m_taskCity.c_str(), m_acqDate.c_str());
	m_pLog->Output("[Acquire_YC] 重设采集 (HIVE) 目标表名为: %s", m_taskInfo.EtlRuleTarget.c_str());
}

void Acquire_YC::HandleYCInfo() throw(base::Exception)
{
	if ( m_vecYCInfo.empty() )
	{
		throw base::Exception(ACQERR_HANDLE_YCINFO_FAILED, "没有业财稽核因子规则信息! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	const int VEC_YCI_SIZE = m_vecYCInfo.size();
	for ( int i = 0; i < VEC_YCI_SIZE; ++i )
	{
		int seq = i + 1;
		YCInfo& ref_yci = m_vecYCInfo[i];

		base::PubStr::TrimUpper(ref_yci.stat_dimid);
		if ( ref_yci.stat_dimid.empty() )
		{
			throw base::Exception(ACQERR_HANDLE_YCINFO_FAILED, "业财稽核统计因子维度ID为空值！[INDEX:%d] (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", seq, m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
		}

		m_pLog->Output("[Acquire_YC] 原业财统计因子 SQL(%d) [%s]: %s", seq, ref_yci.stat_dimid.c_str(), ref_yci.stat_sql.c_str());
		ExchangeSQLMark(ref_yci.stat_sql);
		ExtendSQLCondition(ref_yci.stat_sql);
		CityMarkExchange(ref_yci.stat_sql);		// 地市标记转换
		m_pLog->Output("[Acquire_YC] (转换后) 新统计因子 SQL(%d) [%s]: %s", seq, ref_yci.stat_dimid.c_str(), ref_yci.stat_sql.c_str());
	}
}

void Acquire_YC::ExtendSQLCondition(std::string& sql) throw(base::Exception)
{
	// 不需要进行扩展SQL语句的条件？
	if ( NoNeedExtendSQL(sql) )
	{
		// 删除标记: [NO_NEED_EXTEND]
		sql.erase(0, sql.find(']')+1);
		return;
	}

	const std::string EX_SUB_COND;
	const std::string EXTEND_SQL_COND;
	base::PubStr::SetFormatString(const_cast<std::string&>(EX_SUB_COND), "%s = '%s' and %s = '%s'", m_fieldPeriod.c_str(), m_acqDate.c_str(), m_fieldCity.c_str(), m_taskCity.c_str());
	base::PubStr::SetFormatString(const_cast<std::string&>(EXTEND_SQL_COND), "%s and decimal(%s,12,2) in (select max(decimal(%s,12,2)) from ", EX_SUB_COND.c_str(), m_fieldBatch.c_str(), m_fieldBatch.c_str());

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
			w_pos   = f_pos + off + tab_src.size();

			if ( ')' == tab_src[tab_src.size()-1] )		// 表名后跟')'
			{
				tab_src.erase(tab_src.size()-1);
				w_pos -= 1;
			}

			base::PubStr::SetFormatString(str_add, " where %s %s where %s)", EXTEND_SQL_COND.c_str(), tab_src.c_str(), EX_SUB_COND.c_str());
			sql.insert(w_pos, str_add);
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

			if ( ')' == tab_src[tab_src.size()-1] )		// 表名后跟')'
			{
				tab_src.erase(tab_src.size()-1);
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

bool Acquire_YC::NoNeedExtendSQL(const std::string& sql)
{
	const std::string F_NO_NEED_EX = S_NO_NEED_EXTEND;
	const size_t F_NNEX_SIZE       = F_NO_NEED_EX.size();
	const std::string C_SQL        = base::PubStr::TrimUpperB(sql);

	// 是否以'[NO_NEED_EXTEND]'开头？
	return (C_SQL.size() >= F_NNEX_SIZE && C_SQL.substr(0, F_NNEX_SIZE) == F_NO_NEED_EX);
}

void Acquire_YC::CityMarkExchange(std::string& sql)
{
	const std::string F_CITY_MARK    = S_CITY_MARK;
	const std::string F_CN_CITY_MARK = S_CN_CITY_MARK;

	const size_t F_CTMK_SIZE    = F_CITY_MARK.size();
	const size_t F_CN_CTMK_SIZE = F_CN_CITY_MARK.size();

	// 任务标记（编码）转换
	size_t      pos    = 0;
	std::string up_sql = base::PubStr::UpperB(sql);
	while ( (pos = up_sql.find(F_CITY_MARK)) != std::string::npos )
	{
		sql.replace(pos, F_CTMK_SIZE, m_taskCity);
		up_sql = base::PubStr::UpperB(sql);
	}

	// 任务标记（中文名称）转换
	pos    = 0;
	up_sql = base::PubStr::UpperB(sql);
	while ( (pos = up_sql.find(F_CN_CITY_MARK)) != std::string::npos )
	{
		sql.replace(pos, F_CN_CTMK_SIZE, m_taskCityCN);
		up_sql = base::PubStr::UpperB(sql);
	}
}

void Acquire_YC::MakeYCResultComplete(const std::string& dim, const int& fields, std::vector<std::vector<std::string> >& vec_result)
{
	const int VEC2_SIZE = vec_result.size();
	m_pLog->Output("[Acquire_YC] 业财采集结果：DIM=[%s], RESULT_SIZE=[%d]", dim.c_str(), VEC2_SIZE);

	// 业财采集结果维度 ID 补全
	if ( dim.find('?') != std::string::npos )	// 一般分类因子
	{
		m_pLog->Output("[Acquire_YC] 此为分类因子：保留所有采集结果");
		for ( int i = 0; i < VEC2_SIZE; ++i )
		{
			std::vector<std::string>& ref_vec = vec_result[i];
			ref_vec.insert(ref_vec.begin(), dim);
		}
	}
	else
	{
		if ( 1 == VEC2_SIZE )	// 一个结果
		{
			vec_result[0].insert(vec_result[0].begin(), dim);
		}
		else if ( VEC2_SIZE > 1 )	// 多个结果
		{
			// 仅保留第一个结果
			m_pLog->Output("[Acquire_YC] 多个采集结果：仅保留第一个结果");
			vec_result.erase(vec_result.begin()+1, vec_result.end());
			vec_result[0].insert(vec_result[0].begin(), dim);
		}
		else	// 没有结果
		{
			// 添加一个默认结果
			m_pLog->Output("[Acquire_YC] 没有采集结果：添加一个默认结果");
			std::vector<std::string> vec_default;
			vec_default.push_back(dim);
			for ( int i = 0; i < fields; ++i )
			{
				vec_default.push_back("");
			}
			vec_result.push_back(vec_default);
		}
	}
}

