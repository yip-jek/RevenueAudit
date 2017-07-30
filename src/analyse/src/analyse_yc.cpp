#include "analyse_yc.h"
#include "log.h"
#include "pubstr.h"
#include "simpletime.h"
#include "canadb2.h"
#include "canahive.h"


Analyse_YC::Analyse_YC()
{
	m_sType = "业财稽核";
}

Analyse_YC::~Analyse_YC()
{
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

void Analyse_YC::GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception)
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

void Analyse_YC::AnalyseRules(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	if ( m_taskInfo.AnaRule.AnaType != AnalyseRule::ANATYPE_YC_STAT )	// 业财稽核统计类型
	{
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "不支持的业财稽核分析规则类型: %d (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.AnaRule.AnaType, m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	const std::string ANA_EXP = base::PubStr::TrimB(m_taskInfo.AnaRule.AnaExpress);
	m_pLog->Output("[Analyse_YC] 分析规则类型：业财稽核统计 (KPI_ID:%s, ANA_ID:%s)", m_sKpiID.c_str(), m_sAnaID.c_str());
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
	m_statFactor.LoadStatInfo(vec_ycsinfo);
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
	// 查询统计结果表已存在的最新批次
	YCStatBatch st_batch;
	st_batch.stat_report = m_statFactor.GetStatReport();
	st_batch.stat_id     = m_statFactor.GetStatID();
	st_batch.stat_date   = m_dbinfo.GetEtlDay();
	st_batch.stat_city   = m_taskReq.task_city;
	st_batch.stat_batch  = 0;
	m_pAnaDB2->SelectStatResultMaxBatch(m_dbinfo.target_table, st_batch);
	m_pLog->Output("[Analyse_YC] 统计结果表的最新批次: %d", st_batch.stat_batch);

	// 生成当前统计结果的批次
	m_taskReq.task_batch = st_batch.stat_batch + 1;
	m_pLog->Output("[Analyse_YC] 因此，当前统计结果的批次为: %d", m_taskReq.task_batch);
}

void Analyse_YC::ConvertStatFactor() throw(base::Exception)
{
	int conv_size = m_statFactor.LoadDimFactor(m_v3HiveSrcData);
	m_pLog->Output("[Analyse_YC] 统计因子转换大小：%d", conv_size);

	if ( conv_size < 1 )
	{
		throw base::Exception(ANAERR_TRANS_YCFACTOR_FAILED, "缺少业财稽核统计源数据! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	// 释放Hive源数据
	std::vector<std::vector<std::vector<std::string> > >().swap(m_v3HiveSrcData);
}

void Analyse_YC::GenerateResultData() throw(base::Exception)
{
	std::vector<std::vector<std::string> > vec_yc_data;
	m_statFactor.GenerateResult(m_taskReq.task_batch, m_taskReq.task_city, vec_yc_data);
	if ( vec_yc_data.empty() )
	{
		throw base::Exception(ANAERR_GENERATE_YCDATA_FAILED, "生成报表结果数据失败！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	// (1) 插入业财稽核报表结果数据
	base::PubStr::VVVectorSwapPushBack(m_v3HiveSrcData, vec_yc_data);

	// (2) 生成业财稽核差异汇总结果数据
	// 不一定有差异汇总数据，因此不判断是否为空！
	m_statFactor.GenerateDiffSummaryResult(m_taskReq.task_city, m_vec2DiffSummary);
}

void Analyse_YC::StoreResult() throw(base::Exception)
{
	// 保留旧的数据，所以不执行删除操作！
	// ----RemoveOldResult(m_taskInfo.ResultType);

	if ( m_v3HiveSrcData.empty() )
	{
		throw base::Exception(ANAERR_STORE_RESULT_FAILED, "无法完成入库：业财稽核结果数据不全！(KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	// (1）入库业财稽核报表结果数据
	m_pLog->Output("[Analyse_YC] 准备入库：(1) 业财稽核报表结果数据");
	m_pAnaDB2->InsertResultData(m_dbinfo, m_v3HiveSrcData[0]);

	// (2）入库业财稽核差异汇总结果数据
	m_pLog->Output("[Analyse_YC] 准备入库：(2) 业财稽核差异汇总结果数据");
	StoreDiffSummaryResult();

	// 记录业财稽核日志
	RecordStatisticsLog();

	// 删除采集 (HIVE) 目标表
	DropEtlTargetTable();
}

void Analyse_YC::StoreDiffSummaryResult() throw(base::Exception)
{
	YCStatResult yc_sr;
	const int VEC2_SIZE = m_vec2DiffSummary.size();
	for ( int i = 0; i < VEC2_SIZE; ++i )
	{
		std::vector<std::string>& ref_vec = m_vec2DiffSummary[i];
		if ( !yc_sr.LoadFromVector(ref_vec) )
		{
			throw base::Exception(ANAERR_STORE_DIFF_SUMMARY, "差异汇总结果数据还原失败！[INDEX:%d] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", (i+1), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}

		m_pAnaDB2->UpdateInsertYCDIffSummary(m_dbinfo, yc_sr);
	}

	m_pLog->Output("[Analyse_YC] Store diff summary result data, size: %d", VEC2_SIZE);
}

void Analyse_YC::RecordStatisticsLog()
{
	YCStatLog yc_log;
	yc_log.stat_report     = m_statFactor.GetStatReport();
	yc_log.stat_batch      = m_taskReq.task_batch;
	yc_log.stat_city       = m_taskReq.task_city;
	yc_log.stat_cycle      = m_dbinfo.GetEtlDay();
	yc_log.stat_time       = base::SimpleTime::Now().Time14();	// 当前时间

	std::vector<std::string> vec_datasrc;
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

