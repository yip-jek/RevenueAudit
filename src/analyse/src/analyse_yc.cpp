#include "analyse_yc.h"
#include "log.h"
#include "pubstr.h"
#include "simpletime.h"
#include "canadb2.h"
#include "canahive.h"
#include "ycstatfactor_hdb.h"
#include "ycstatfactor_xqb.h"


Analyse_YC::Analyse_YC()
:m_pStatFactor(NULL)
{
	m_sType = "业财稽核";
}

Analyse_YC::~Analyse_YC()
{
	ReleaseStatFactor();
}

void Analyse_YC::LoadConfig() throw(base::Exception)
{
	Analyse::LoadConfig();

	m_cfg.RegisterItem("TABLE", "TAB_YC_TASK_REQ");
	m_cfg.RegisterItem("TABLE", "TAB_YCRA_STATRULE");
	m_cfg.RegisterItem("TABLE", "TAB_YCRA_STATLOG");
	m_cfg.RegisterItem("TABLE", "TAB_YCRA_REPORTSTAT");
	m_cfg.RegisterItem("TABLE", "TAB_YCRA_PROCESSLOG");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_PERIOD");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_CITY");
	m_cfg.RegisterItem("FIELD", "SRC_FIELD_BATCH");

	m_cfg.ReadConfig();

	m_tabYCTaskReq  = m_cfg.GetCfgValue("TABLE", "TAB_YC_TASK_REQ");
	m_tabStatRule   = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_STATRULE");
	m_tabStatLog    = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_STATLOG");
	m_tabReportStat = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_REPORTSTAT");
	m_tabProcessLog = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_PROCESSLOG");
	m_fieldPeriod   = m_cfg.GetCfgValue("FIELD", "SRC_FIELD_PERIOD");
	m_fieldCity     = m_cfg.GetCfgValue("FIELD", "SRC_FIELD_CITY");
	m_fieldBatch    = m_cfg.GetCfgValue("FIELD", "SRC_FIELD_BATCH");

	m_pLog->Output("[Analyse_YC] Load configuration OK.");
}

std::string Analyse_YC::GetLogFilePrefix()
{
	return std::string("Analyse_YC");
}

void Analyse_YC::Init() throw(base::Exception)
{
	Analyse::Init();

	m_pAnaDB2->SetTabYCTaskReq(m_tabYCTaskReq);
	m_pAnaDB2->SetTabYCStatRule(m_tabStatRule);
	m_pAnaDB2->SetTabYCStatLog(m_tabStatLog);
	m_pAnaDB2->SetTabYCReportStat(m_tabReportStat);
	m_pAnaDB2->SetTabYCProcessLog(m_tabProcessLog);

	// 更新任务状态为："21"（正在分析）
	m_taskReq.state      = "21";
	m_taskReq.state_desc = "正在分析";
	m_taskReq.task_batch = -1;
	m_taskReq.task_desc  = "分析开始时间：" + base::SimpleTime::Now().TimeStamp();
	m_pAnaDB2->UpdateYCTaskReq(m_taskReq);

	m_pLog->Output("[Analyse_YC] Init OK.");
}

void Analyse_YC::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	// 更新任务状态
	if ( 0 == err_code )	// 正常退出
	{
		// 更新任务状态为："22"（分析完成）
		m_taskReq.state      = "22";
		m_taskReq.state_desc = "分析完成";
		m_taskReq.task_desc  = "分析结束时间：" + base::SimpleTime::Now().TimeStamp();
	}
	else	// 异常退出
	{
		// 更新任务状态为："23"（分析失败）
		m_taskReq.state      = "23";
		m_taskReq.state_desc = "分析失败";
		m_taskReq.task_batch = -1;
		base::PubStr::SetFormatString(m_taskReq.task_desc, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
	}
	m_pAnaDB2->UpdateYCTaskReq(m_taskReq);

	Analyse::End(err_code, err_msg);
}

void Analyse_YC::GetExtendParaTaskInfo(VEC_STRING& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < 4 )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法拆分出业财任务流水号! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::Str2Int(vec_str[3], m_taskReq.seq) )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "无效的业财任务流水号：%s [FILE:%s, LINE:%d]", vec_str[3].c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Analyse_YC] 业财稽核任务流水号：%d", m_taskReq.seq);
}

void Analyse_YC::AnalyseRules(VEC_STRING& vec_hivesql) throw(base::Exception)
{
	// 是否为业财稽核类型
	std::string cn_anatype;
	if ( !CheckYCAnalyseType(cn_anatype) )
	{
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "不支持的业财稽核分析规则类型: %d (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.AnaRule.AnaType, m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	const std::string ANA_EXP = base::PubStr::TrimB(m_taskInfo.AnaRule.AnaExpress);
	m_pLog->Output("[Analyse_YC] 分析规则类型：%s (KPI_ID:%s, ANA_ID:%s)", cn_anatype.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str());
	m_pLog->Output("[Analyse_YC] 分析规则表达式：%s", ANA_EXP.c_str());

	// 生成数据库[DB2]信息
	GetAnaDBInfo();

	// 业财稽核只有一个采集与一个分析
	// 重设采集 (HIVE) 目标表名：加上地市与账期
	std::string& ref_target = m_taskInfo.vecEtlRule[0].TargetPatch;
	base::PubStr::SetFormatString(ref_target, "%s_%s_%s", ref_target.c_str(), m_taskReq.task_city.c_str(), m_dbinfo.GetEtlDay().c_str());
	m_pLog->Output("[Analyse_YC] 重置采集 (HIVE) 目标表名为: %s", ref_target.c_str());

	GetStatisticsHiveSQL(vec_hivesql);
}

bool Analyse_YC::CheckYCAnalyseType(std::string& cn_type) const
{
	switch ( m_taskInfo.AnaRule.AnaType )
	{
	case AnalyseRule::ANATYPE_YCHDB:
		cn_type = "业财核对表稽核";
		return true;
	case AnalyseRule::ANATYPE_YCXQB_YW:
		cn_type = "业财详情表（业务侧）稽核";
		return true;
	case AnalyseRule::ANATYPE_YCXQB_CW:
		cn_type = "业财详情表（财务侧）稽核";
		return true;
	case AnalyseRule::ANATYPE_YCXQB_GD:
		cn_type = "业财详情表（省）稽核";
		return true;
	default:
		return false;
	}
}

void Analyse_YC::ReleaseStatFactor()
{
	if ( m_pStatFactor != NULL )
	{
		delete m_pStatFactor;
		m_pStatFactor = NULL;
	}
}

void Analyse_YC::FetchTaskInfo() throw(base::Exception)
{
	Analyse::FetchTaskInfo();

	// 获取任务地市信息
	m_pAnaDB2->SelectYCTaskReq(m_taskReq);
	base::PubStr::Trim(m_taskReq.task_city);
	m_pLog->Output("[Analyse_YC] Get task request: %s", m_taskReq.LogPrintInfo().c_str());

	m_pLog->Output("[Analyse_YC] 获取业财稽核因子规则信息 ...");
	std::vector<YCStatInfo> vec_ycsinfo;
	m_pAnaDB2->SelectYCStatRule(m_sKpiID, vec_ycsinfo);

	CreateStatFactor();
	m_pStatFactor->LoadStatInfo(vec_ycsinfo);
}

void Analyse_YC::CreateStatFactor() throw(base::Exception)
{
	ReleaseStatFactor();

	if ( AnalyseRule::ANATYPE_YCHDB == m_taskInfo.AnaRule.AnaType )	// 核对表
	{
		m_pStatFactor = new YCStatFactor_HDB(m_taskReq);
	}
	else	// 详情表
	{
		m_pStatFactor = new YCStatFactor_XQB(m_taskReq);
	}

	if ( NULL == m_pStatFactor )
	{
		throw base::Exception(ANAERR_CREATE_STATFACTOR, "Operator new YCStatFactor failed: 无法申请到内存空间！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}
}

void Analyse_YC::AnalyseSourceData() throw(base::Exception)
{
	//Analyse::AnalyseSourceData();

	GenerateNewBatch();

	ConvertStatFactor();

	GenerateResultData();

	// 生成了业财稽核的统计数据，再进行数据补全
	DataSupplement();
}

void Analyse_YC::GenerateNewBatch()
{
	if ( AnalyseRule::ANATYPE_YCHDB == m_taskInfo.AnaRule.AnaType )	// 核对表
	{
		SetNewBatch_HDB();
	}
	else	// 详情表
	{
		SetNewBatch_XQB();
	}
}

void Analyse_YC::SetNewBatch_HDB()
{
	// 查询地市核对表的最新批次
	YCHDBBatch hd_batch;
	hd_batch.stat_report = m_pStatFactor->GetStatReport();
	hd_batch.stat_id     = m_pStatFactor->GetStatID();
	hd_batch.stat_date   = m_dbinfo.GetEtlDay();
	hd_batch.stat_city   = m_taskReq.task_city;
	hd_batch.stat_batch  = 0;
	m_pAnaDB2->SelectHDBMaxBatch(m_dbinfo.target_table, hd_batch);
	m_pLog->Output("[Analyse_YC] 地市核对表的最新批次: %d", hd_batch.stat_batch);

	// 生成当前统计结果的批次
	m_taskReq.task_batch = hd_batch.stat_batch + 1;
	m_pLog->Output("[Analyse_YC] 因此，当前统计结果的批次为: %d", m_taskReq.task_batch);
}

void Analyse_YC::SetNewBatch_XQB() throw(base::Exception)
{
	// 查询地市详情表的最新批次
	YCXQBBatch xq_batch;
	xq_batch.bill_cyc   = m_dbinfo.GetEtlDay().substr(0, 6);
	xq_batch.city       = m_taskReq.task_city;
	xq_batch.type       = "0";			// 类型：0-固定项，1-浮动项
	xq_batch.busi_batch = 0;
	m_pAnaDB2->SelectXQBMaxBatch(m_dbinfo.target_table, xq_batch);
	m_pLog->Output("[Analyse_YC] 地市详情表的最新批次: %d", xq_batch.busi_batch);

	// 生成当前统计结果的批次
	if ( AnalyseRule::ANATYPE_YCXQB_CW == m_taskInfo.AnaRule.AnaType )	
	{
		// 财务侧批次与业务侧批次保持一致
		if ( xq_batch.busi_batch > 0 )		// 有效批次
		{
			m_taskReq.task_batch = xq_batch.busi_batch;
			m_pLog->Output("[Analyse_YC] 与业务侧批次保持一致，因此当前财务侧批次为: %d", m_taskReq.task_batch);
		}
		else	// 无效批次
		{
			throw base::Exception(ANAERR_SET_NEWBATCH_XQB, "没有详情表业务侧的批次信息：%s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", xq_batch.LogPrintInfo().c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}
	}
	else
	{
		m_taskReq.task_batch = xq_batch.busi_batch + 1;
		m_pLog->Output("[Analyse_YC] 因此，当前统计结果的批次为: %d", m_taskReq.task_batch);
	}
}

void Analyse_YC::ConvertStatFactor() throw(base::Exception)
{
	int conv_size = m_pStatFactor->LoadFactors(m_v3HiveSrcData);
	m_pLog->Output("[Analyse_YC] 统计因子转换大小：%d", conv_size);

	if ( conv_size < 1 )
	{
		throw base::Exception(ANAERR_TRANS_YCFACTOR_FAILED, "缺少业财稽核统计源数据! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
}

void Analyse_YC::GenerateResultData() throw(base::Exception)
{
	m_pStatFactor->MakeResult(m_v3HiveSrcData);

	if ( base::PubStr::CalcVVVectorStr(m_v3HiveSrcData) == 0 )
	{
		throw base::Exception(ANAERR_GENERATE_YCDATA_FAILED, "生成报表结果数据失败！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	// 提取差异汇总数据
	if ( AnalyseRule::ANATYPE_YCHDB == m_taskInfo.AnaRule.AnaType )	// 核对表
	{
		m_v2DiffSummary.swap(m_v3HiveSrcData[1]);
		m_v3HiveSrcData.erase(m_v3HiveSrcData.begin()+1);
	}
}

void Analyse_YC::StoreResult() throw(base::Exception)
{
	// 保留旧的数据，所以不执行删除操作！
	// ----RemoveOldResult(m_taskInfo.ResultType);

	if ( m_v3HiveSrcData.empty() )
	{
		throw base::Exception(ANAERR_STORE_RESULT_FAILED, "无法完成入库：缺少业财稽核结果数据！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	// 入库业财报表稽核结果
	StoreReportResult();

	// 登记信息：日志信息、报表状态、流程记录 等
	RecordInformation();

	// 删除采集 (HIVE) 目标表
	DropEtlTargetTable();
}

void Analyse_YC::StoreReportResult()
{
	// 详情表（财务侧）结果数据更新到数据库
	if ( AnalyseRule::ANATYPE_YCXQB_CW == m_taskInfo.AnaRule.AnaType )
	{
		m_pLog->Output("[Analyse_YC] 准备入库：业财稽核详情表（财务侧）结果数据");
	}
	else	// 非详情表（财务侧）数据
	{
		m_pLog->Output("[Analyse_YC] 准备入库：业财报表稽核统计结果数据");
		m_pAnaDB2->InsertResultData(m_dbinfo, m_v3HiveSrcData[0]);

		if ( AnalyseRule::ANATYPE_YCHDB == m_taskInfo.AnaRule.AnaType )	// 核对表
		{
			m_pLog->Output("[Analyse_YC] 准备入库：业财稽核差异汇总结果数据");
			StoreDiffSummaryResult();
		}
	}
}

void Analyse_YC::StoreDiffSummaryResult() throw(base::Exception)
{
	YCStatResult yc_sr;

	const int VEC2_SIZE = m_v2DiffSummary.size();
	for ( int i = 0; i < VEC2_SIZE; ++i )
	{
		VEC_STRING& ref_vec = m_v2DiffSummary[i];
		if ( !yc_sr.Import(ref_vec) )
		{
			throw base::Exception(ANAERR_STORE_DIFF_SUMMARY, "差异汇总结果数据还原失败！[INDEX:%d] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", (i+1), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}

		m_pAnaDB2->UpdateInsertYCDIffSummary(m_dbinfo, yc_sr);
	}

	m_pLog->Output("[Analyse_YC] Store diff summary result data size: %d", VEC2_SIZE);
}

void Analyse_YC::RecordInformation()
{
	if ( AnalyseRule::ANATYPE_YCHDB == m_taskInfo.AnaRule.AnaType )	// 核对表
	{
		// 记录业财稽核日志
		RecordStatisticsLog();
	}

	// 登记报表状态
	YCReportState report_state;
	RecordReportState(report_state);

	// 登记流程记录日志
	RecordProcessLog(report_state);
}

void Analyse_YC::RecordStatisticsLog()
{
	YCStatLog yc_log;
	yc_log.stat_report = m_pStatFactor->GetStatReport();
	yc_log.stat_batch  = m_taskReq.task_batch;
	yc_log.stat_city   = m_taskReq.task_city;
	yc_log.stat_cycle  = m_dbinfo.GetEtlDay();
	yc_log.stat_time   = base::SimpleTime::Now().Time14();	// 当前时间

	VEC_STRING vec_datasrc;
	base::PubStr::Str2StrVector(m_taskInfo.vecEtlRule[0].DataSource, ",", vec_datasrc);

	YCSrcInfo yc_srcinfo;
	yc_srcinfo.field_period = m_fieldPeriod;
	yc_srcinfo.period       = yc_log.stat_cycle;
	yc_srcinfo.field_city   = m_fieldCity;
	yc_srcinfo.city         = m_taskReq.task_city;
	yc_srcinfo.field_batch  = m_fieldBatch;
	yc_srcinfo.batch        = 0;

	// 稽核源数据及批次
	const int VEC_SIZE = vec_datasrc.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		std::string& ref_src = vec_datasrc[i];
		yc_srcinfo.src_tab = ref_src;

		m_pAnaDB2->SelectYCSrcMaxBatch(yc_srcinfo);
		m_pLog->Output("[Analyse_YC] 取得数据源表(%d): %s, 地市: %s, 最新批次: %d", (i+1), ref_src.c_str(), m_taskReq.task_city.c_str(), yc_srcinfo.batch);

		if ( i != 0 )	// Not first
		{
			yc_log.stat_datasource += ("|" + ref_src + "," + base::PubStr::Int2Str(yc_srcinfo.batch));
		}
		else	// First one
		{
			yc_log.stat_datasource = ref_src + "," + base::PubStr::Int2Str(yc_srcinfo.batch);
		}
	}

	m_pLog->Output("[Analyse_YC] 更新业财稽核记录日志表");
	m_pAnaDB2->InsertYCStatLog(yc_log);
}

void Analyse_YC::RecordReportState(YCReportState& report_state)
{
	report_state.report_id = m_pStatFactor->GetStatReport();
	report_state.bill_cyc  = m_dbinfo.GetEtlDay().substr(0, 6);
	report_state.status    = "00";			// 状态：00-待审核
	report_state.actor     = m_taskReq.actor;

	// 业财详情表（省）稽核：地市指定为"GD"
	if ( AnalyseRule::ANATYPE_YCXQB_GD == m_taskInfo.AnaRule.AnaType )
	{
		report_state.city = "GD";
	}
	else
	{
		report_state.city = m_taskReq.task_city;
	}

	// 类型：00-核对表，01-地市账务业务详情表，02-监控表，03-地市财务对账详情表
	if ( AnalyseRule::ANATYPE_YCHDB == m_taskInfo.AnaRule.AnaType )	// 核对表
	{
		report_state.type = "00";
	}
	else	// 详情表
	{
		report_state.type = "01";
	}

	m_pAnaDB2->UpdateInsertReportState(report_state);
}

void Analyse_YC::RecordProcessLog(const YCReportState& report_state)
{
	YCProcessLog proc_log;
	proc_log.report_id = report_state.report_id;
	proc_log.bill_cyc  = report_state.bill_cyc;
	proc_log.city      = report_state.city;
	proc_log.status    = report_state.status;
	proc_log.type      = report_state.type;
	proc_log.actor     = report_state.actor;
	proc_log.oper      = m_taskReq.oper;
	proc_log.version   = m_taskReq.task_batch;
	proc_log.uptime    = base::SimpleTime::Now().Time14();

	m_pAnaDB2->UpdateInsertProcessLogState(proc_log);
}

void Analyse_YC::DropEtlTargetTable()
{
	std::string& ref_target = m_taskInfo.vecEtlRule[0].TargetPatch;
	m_pLog->Output("[Analyse_YC] 删除采集 (HIVE) 目标表: %s", ref_target.c_str());

	std::string drop_sql = "DROP TABLE IF EXISTS " + ref_target;
	m_pAnaHive->ExecuteAnaSQL(drop_sql);
}

void Analyse_YC::UpdateDimValue()
{
	// 业财稽核统计，无需更新维度取值范围！
	m_pLog->Output("[Analyse_YC] 无需更新维度取值范围！");
}

