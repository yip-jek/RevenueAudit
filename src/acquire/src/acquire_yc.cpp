#include "acquire_yc.h"
#include "pubstr.h"
#include "log.h"
#include "simpletime.h"
#include "cacqdb2.h"
#include "cacqhive.h"
#include "sqlextendconverter.h"
#include "sqltranslator.h"
#include "acqarithmet.h"

const char* const Acquire_YC::S_YC_ETLRULE_TYPE = "YCRA";				// 业财稽核-采集规则类型
const char* const YCInfo::S_EXTERNAL_SQL        = "[EXTERNAL_SQL]";		// 外部SQL

Acquire_YC::Acquire_YC()
:m_pSqlExConv(NULL)
{
	m_sType = "业财稽核";
}

Acquire_YC::~Acquire_YC()
{
	ReleaseSqlExtendConv();
}

void Acquire_YC::LoadConfig() throw(base::Exception)
{
	Acquire::LoadConfig();

	m_cfg.RegisterItem("TABLE", "TAB_YC_TASK_REQ");
	m_cfg.RegisterItem("TABLE", "TAB_YCRA_STATRULE");
	m_cfg.RegisterItem("TABLE", "TAB_YCRA_REPORTSTAT");
	m_cfg.RegisterItem("TABLE", "TAB_DICT_CITY");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_PERIOD");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_CITY");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_BATCH");

	m_cfg.ReadConfig();

	m_tabYCTaskReq  = m_cfg.GetCfgValue("TABLE", "TAB_YC_TASK_REQ");
	m_tabStatRule   = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_STATRULE");
	m_tabReportStat = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_REPORTSTAT");
	m_tabDictCity   = m_cfg.GetCfgValue("TABLE", "TAB_DICT_CITY");
	m_fieldPeriod   = m_cfg.GetCfgValue("FIELD", "SRC_FIELD_PERIOD");
	m_fieldCity     = m_cfg.GetCfgValue("FIELD", "SRC_FIELD_CITY");
	m_fieldBatch    = m_cfg.GetCfgValue("FIELD", "SRC_FIELD_BATCH");

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
	m_pAcqDB2->SetTabReportState(m_tabReportStat);
	m_pAcqDB2->SetTabYCDictCity(m_tabDictCity);

	// 获取任务账期与地市信息
	m_pAcqDB2->SelectYCTaskRequest(m_taskRequest);

	// 初始化报表状态信息
	InitReportState();

	// 登记报表状态："07"-正在稽核
	RegisterReportState("07");

	// 更新任务状态为：正在采集
	UpdateTaskRequestState(TREQ_STATE_BEGIN);
	m_pLog->Output("[Acquire_YC] Init OK.");
}

void Acquire_YC::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	// 更新任务状态
	if ( 0 == err_code )	// 正常退出
	{
		// 更新任务状态为：采集完成
		UpdateTaskRequestState(TREQ_STATE_SUCCESS);
	}
	else	// 异常退出
	{
		// 登记报表状态："08"-稽核失败
		RegisterReportState("08");

		// 更新任务状态为：采集失败
		base::PubStr::SetFormatString(m_taskRequest.task_desc, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
		UpdateTaskRequestState(TREQ_STATE_FAIL);
	}

	Acquire::End(err_code, err_msg);
}

void Acquire_YC::GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < 4 )
	{
		throw base::Exception(ACQERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法拆分出业财任务流水号! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::Str2Int(vec_str[3], m_taskRequest.seq) )
	{
		throw base::Exception(ACQERR_TASKINFO_ERROR, "无效的业财任务流水号：%s [FILE:%s, LINE:%d]", vec_str[3].c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Acquire_YC] 业财稽核任务流水号：%d", m_taskRequest.seq);
}

void Acquire_YC::InitReportState()
{
	m_reportState.report_id = m_taskRequest.stat_id;
	m_reportState.bill_cyc  = m_taskRequest.bill_cyc;
	m_reportState.city      = m_taskRequest.city;
	m_reportState.type      = GetReportStateType();
	m_reportState.actor     = m_taskRequest.actor;

	// 状态延迟到登记报表状态入库时才设置
	m_reportState.status.clear();
}

std::string Acquire_YC::GetReportStateType()
{
	/// 类型：
	// 00-核对表
	// 01-地市账务业务详情表
	// 02-监控表
	// 03-地市财务对账详情表
	// 04-省财务详情表
	// 05-省财务监控表
	const std::string KPI_TYPE = base::PubStr::TrimUpperB(m_pAcqDB2->SelectKpiRuleType(m_taskRequest.kpi_id));
	if ( KPI_TYPE == "YC_HDB" )				// 业财核对表
	{
		return "00";
	}
	else if ( KPI_TYPE == "YC_XQB_YW" )		// 业财详情表（业务侧）
	{
		return "01";
	}
	else if ( KPI_TYPE == "YC_XQB_CW" )		// 业财详情表（财务侧）
	{
		return "03";
	}
	else if ( KPI_TYPE == "YC_XQB_GD" )		// 业财详情表（省财务侧）
	{
		return "04";
	}
	else		// 未知
	{
		return "";
	}
}

void Acquire_YC::RegisterReportState(const std::string& state)
{
	m_reportState.status = state;
	m_pAcqDB2->UpdateInsertReportState(m_reportState);
}

void Acquire_YC::UpdateTaskRequestState(TASK_REQUEST_STATE t_state)
{
	// 设置任务状态
	switch ( t_state )
	{
	case TREQ_STATE_BEGIN:
		m_taskRequest.task_state = "11";
		m_taskRequest.state_desc = "正在采集";
		m_taskRequest.task_desc  = "采集开始时间：" + base::SimpleTime::Now().TimeStamp();
		break;
	case TREQ_STATE_SUCCESS:
		m_taskRequest.task_state = "12";
		m_taskRequest.state_desc = "采集完成";
		m_taskRequest.task_desc  = "采集结束时间：" + base::SimpleTime::Now().TimeStamp();
		break;
	case TREQ_STATE_FAIL:
		m_taskRequest.task_state = "13";
		m_taskRequest.state_desc = "采集失败";
		//m_taskRequest.task_desc  = "";			// 采集失败的任务备注信息在之前设置
		break;
	case TREQ_STATE_UNKNOWN:		// 未知，错误
	default:
		m_pLog->Output("[Acquire_YC] <WARNING> Update task request state failed! Unknown state: [%d]", t_state);
		return;
	}

	// 更新任务状态
	m_pAcqDB2->UpdateYCTaskReq(m_taskRequest);
}

void Acquire_YC::FetchTaskInfo() throw(base::Exception)
{
	Acquire::FetchTaskInfo();

	m_acqDate = base::PubStr::TrimB(m_taskRequest.bill_cyc);
	m_pLog->Output("[Acquire_YC] Get task request period: [%s]", m_acqDate.c_str());

	base::PubStr::Trim(m_taskRequest.city);
	m_pLog->Output("[Acquire_YC] Get task request city: [%s]", m_taskRequest.city.c_str());

	if ( m_pAcqDB2->SelectYCTaskCityCN(m_taskRequest.city, m_taskCityCN) )
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

	AcqArithmet acq_arith;

	const int VEC_YCI_SIZE = m_vecYCInfo.size();
	for ( int i = 0; i < VEC_YCI_SIZE; ++i )
	{
		YCInfo& ref_yci = m_vecYCInfo[i];

		std::vector<std::vector<std::string> > vec2_data;
		const int VEC_SIZE = ref_yci.vec_statsql.size();
		for ( int j = 0; j < VEC_SIZE; ++j )
		{
			std::string& ref_sql = ref_yci.vec_statsql[j];

			std::vector<std::vector<std::string> > v2_data;
			if ( AcqArithmet::IsArithmetic(ref_sql) )		// 计算表达式
			{
				// 保留计算式，下一步进行运算
				std::vector<std::string> vec_arith;
				vec_arith.push_back(ref_sql);
				v2_data.push_back(vec_arith);
			}
			else	// 采集SQL，非计算表达式
			{
				// 从 DB2 数据库中采集结果数据
				m_pAcqDB2->FetchEtlData(ref_sql, v2_data);
			}

			base::PubStr::MergeVec2Str(vec2_data, v2_data);
		}

		acq_arith.DoCalculate(vec2_data);
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

std::string Acquire_YC::GeneralHdfsFileName()
{
	// 文件名中：增加地市与账期
	std::string hdfs_file_name;
	base::PubStr::SetFormatString(hdfs_file_name, "%s_%s_%s_%s_%s%02d", 
												  m_sKpiID.c_str(), 
												  m_sEtlID.c_str(), 
												  m_taskRequest.city.c_str(), 
												  m_acqDate.c_str(), 
												  base::SimpleTime::Now().Time14().c_str(), 
												  ++m_seqHdfsFile);
	return hdfs_file_name;
}

void Acquire_YC::GenerateEtlDate(const std::string& date_fmt) throw(base::Exception)
{
	std::string acq_date;
	if ( !base::PubTime::DateApartFromNow(date_fmt, m_acqDateType, acq_date) )
	{
		throw base::Exception(ACQERR_GEN_ETL_DATE_FAILED, "采集时间转换失败！无法识别的采集时间表达式：%s [FILE:%s, LINE:%d]", date_fmt.c_str(), __FILE__, __LINE__);
	}

	// 采集账期以任务账期为准！
	if ( base::PubTime::DT_MONTH == m_acqDateType )		// 月
	{
		m_acqDate = m_acqDate.substr(0, 6);
	}
	else if ( base::PubTime::DT_DAY == m_acqDateType )	// 日：取YYYYMM+01
	{
		m_acqDate = m_acqDate.substr(0, 6) + "01";
	}
	else
	{
		throw base::Exception(ACQERR_GEN_ETL_DATE_FAILED, "采集时间转换失败！未知时间类型：%d [FILE:%s, LINE:%d]", m_acqDateType, __FILE__, __LINE__);
	}
	m_pLog->Output("[Acquire_YC] 生成采集账期时间：[%s]", m_acqDate.c_str());

	SQLTransRelease();
	m_pSQLTrans = new base::SQLTranslator(m_acqDateType, m_acqDate);
	if ( NULL == m_pSQLTrans )
	{
		throw base::Exception(ACQERR_GEN_ETL_DATE_FAILED, "new SQLTranslator failed: 无法申请到内存空间! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 重设采集 (HIVE) 目标表名：加上地市与账期
	base::PubStr::SetFormatString(m_taskInfo.EtlRuleTarget, "%s_%s_%s", m_taskInfo.EtlRuleTarget.c_str(), m_taskRequest.city.c_str(), m_acqDate.c_str());
	m_pLog->Output("[Acquire_YC] 重设采集 (HIVE) 目标表名为: %s", m_taskInfo.EtlRuleTarget.c_str());
}

void Acquire_YC::HandleYCInfo() throw(base::Exception)
{
	if ( m_vecYCInfo.empty() )
	{
		throw base::Exception(ACQERR_HANDLE_YCINFO_FAILED, "没有业财稽核因子规则信息! (KPI_ID:%s, ETLRULE_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.KpiID.c_str(), m_taskInfo.EtlRuleID.c_str(), __FILE__, __LINE__);
	}

	CreateSqlExtendConv();

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

		const int VEC_SIZE = ref_yci.vec_statsql.size();
		for ( int j = 0; j < VEC_SIZE; ++j )
		{
			std::string& ref_sql = ref_yci.vec_statsql[j];

			m_pLog->Output("[Acquire_YC] 原业财统计因子 SQL(%d) [%s]: %s", seq, ref_yci.stat_dimid.c_str(), ref_sql.c_str());
			ExchangeSQLMark(ref_sql);
			SQLExtendConvert(ref_sql);
			m_pLog->Output("[Acquire_YC] (转换后) 新统计因子 SQL(%d) [%s]: %s", seq, ref_yci.stat_dimid.c_str(), ref_sql.c_str());
		}
	}
}

void Acquire_YC::CreateSqlExtendConv()
{
	ReleaseSqlExtendConv();

	SQLExtendData sql_ex_data;
	sql_ex_data.field_period = m_fieldPeriod;
	sql_ex_data.field_city   = m_fieldCity;
	sql_ex_data.field_batch  = m_fieldBatch;
	sql_ex_data.period       = m_acqDate.substr(0, 6);		// 账期改为6位，格式：YYYYMM
	sql_ex_data.city         = m_taskRequest.city;
	sql_ex_data.cityCN       = m_taskCityCN;

	m_pSqlExConv = new SQLExtendConverter(sql_ex_data);
	if ( NULL == m_pSqlExConv )
	{
		throw base::Exception(ACQERR_CREATE_SQLEXTENDCONV, "Operator new SQLExtendConverter failed: 无法申请到内存空间！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}
}

void Acquire_YC::ReleaseSqlExtendConv()
{
	if ( m_pSqlExConv != NULL )
	{
		delete m_pSqlExConv;
		m_pSqlExConv = NULL;
	}
}

void Acquire_YC::SQLExtendConvert(std::string& sql)
{
	m_pSqlExConv->Extend(sql);
	m_pSqlExConv->CityConvert(sql);
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

