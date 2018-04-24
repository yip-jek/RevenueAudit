#include "analyse_yc.h"
#include "log.h"
#include "pubstr.h"
#include "simpletime.h"
#include "ycanadb2.h"
#include "canahive.h"
#include "ycresult_xqb.h"
#include "ycstatfactor_hdb.h"
#include "ycstatfactor_xqb.h"


Analyse_YC::Analyse_YC()
:m_pYCDB2(NULL)
,m_pStatFactor(NULL)
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

	m_pYCDB2->SetTabYCTaskReq(m_tabYCTaskReq);
	m_pYCDB2->SetTabYCStatRule(m_tabStatRule);
	m_pYCDB2->SetTabYCStatLog(m_tabStatLog);
	m_pYCDB2->SetTabYCReportStat(m_tabReportStat);
	m_pYCDB2->SetTabYCProcessLog(m_tabProcessLog);

	// 更新任务状态为："21"（正在分析）
	m_taskReq.state      = "21";
	m_taskReq.state_desc = "正在分析";
	m_taskReq.task_batch = -1;
	m_taskReq.task_desc  = "分析开始时间：" + base::SimpleTime::Now().TimeStamp();
	m_pYCDB2->UpdateYCTaskReq(m_taskReq);

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
		// 登记报表状态："08"-稽核失败
		RecordReportState("08");

		// 更新任务状态为："23"（分析失败）
		m_taskReq.state      = "23";
		m_taskReq.state_desc = "分析失败";
		m_taskReq.task_batch = -1;
		base::PubStr::SetFormatString(m_taskReq.task_desc, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
	}
	m_pYCDB2->UpdateYCTaskReq(m_taskReq);

	Analyse::End(err_code, err_msg);
}

CAnaDB2* Analyse_YC::CreateDBConnection() throw(base::Exception)
{
	return (m_pYCDB2 = new YCAnaDB2(m_sDBName, m_sUsrName, m_sPasswd));
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

	RemoveSumFunc(vec_hivesql);
}

void Analyse_YC::RemoveSumFunc(VEC_STRING& vec_hivesql)
{
	std::string UPPER_SQL;
	size_t pos = 0;

	// Remove the SQL function: sum()
	const int VEC_SIZE = vec_hivesql.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		std::string& ref_sql = vec_hivesql[i];

		UPPER_SQL = base::PubStr::UpperB(ref_sql);
		while ( (pos = UPPER_SQL.find(" SUM(")) != std::string::npos )
		{
			UPPER_SQL.erase(pos+1, 4);
			ref_sql.erase(pos+1, 4);

			pos = UPPER_SQL.find(')');
			if ( std::string::npos == pos )
			{
				break;
			}

			UPPER_SQL.erase(pos, 1);
			ref_sql.erase(pos, 1);
		}

		// Remove the SQL function: group by ...
		if ( (pos = UPPER_SQL.find(" GROUP BY ")) != std::string::npos )
		{
			ref_sql.erase(pos);
		}
	}
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

void Analyse_YC::GetAnaDBInfo() throw(base::Exception)
{
	Analyse::GetAnaDBInfo();

	// 详情表（财务侧）采用更新SQL语句
	if ( AnalyseRule::ANATYPE_YCXQB_CW == m_taskInfo.AnaRule.AnaType )
	{
		int item_size = 0;
		int val_size  = 0;
		GetFactorItemValSize(item_size, val_size);

		const int MEMBER_SIZE = YCResult_XQB::S_PUBLIC_MEMBERS + item_size + val_size + 3;
		const int FIELD_SIZE  = m_dbinfo.GetFieldSize();
		if ( FIELD_SIZE != MEMBER_SIZE )
		{
			throw base::Exception(ANAERR_GET_DBINFO_FAILED, "指标字段数不足[%d]: %d [FILE:%s, LINE:%d]", MEMBER_SIZE, FIELD_SIZE, __FILE__, __LINE__);
		}

		// 更新 SQL 语句头
		m_dbinfo.db2_sql  = "UPDATE " + m_dbinfo.target_table + " SET ";

		// SET 的内容
		const int COND_FIELD_SIZE = YCResult_XQB::S_PUBLIC_MEMBERS + 1;
		for ( int i = COND_FIELD_SIZE; i < FIELD_SIZE; ++i )
		{
			m_dbinfo.db2_sql += m_dbinfo.GetAnaField(i).field_name + " = ?, ";
		}
		// 加入关键词 WHERE
		m_dbinfo.db2_sql.replace(m_dbinfo.db2_sql.size()-2, 1, " WHERE");

		// WHERE 后的条件
		m_dbinfo.db2_sql += m_dbinfo.GetAnaField(0).field_name + " = ?";
		for ( int i = 1; i < COND_FIELD_SIZE; ++i )
		{
			m_dbinfo.db2_sql += " AND " + m_dbinfo.GetAnaField(i).field_name + " = ?";
		}
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
	// (首先) 获取任务信息
	m_pYCDB2->SelectYCTaskReq(m_taskReq);
	base::PubStr::Trim(m_taskReq.task_city);
	m_pLog->Output("[Analyse_YC] Get task request: %s", m_taskReq.LogPrintInfo().c_str());

	Analyse::FetchTaskInfo();

	// 获取（详情表-业务侧）更新字段列表
	FetchUpdateFields_YW();

	m_pLog->Output("[Analyse_YC] 获取业财稽核因子规则信息 ...");
	std::vector<YCStatInfo> vec_ycsinfo;
	m_pYCDB2->SelectYCStatRule(m_sKpiID, vec_ycsinfo);

	CreateStatFactor();
	m_pStatFactor->LoadStatInfo(vec_ycsinfo);
}

void Analyse_YC::EtlTimeConvertion() throw(base::Exception)
{
	// 任务账期
	const std::string TASK_CYCLE = base::PubStr::TrimB(m_taskReq.stat_cycle);

	// 任务年份
	int year  = 0;
	if ( !base::PubStr::Str2Int(TASK_CYCLE.substr(0, 4), year) )
	{
		throw base::Exception(ANAERR_ETLTIME_CONVERTION, "采集时间转换失败！无效的任务账期时间：%s [FILE:%s, LINE:%d]", TASK_CYCLE.c_str(), __FILE__, __LINE__);
	}

	// 任务月份
	int mon = 0;
	if ( !base::PubStr::Str2Int(TASK_CYCLE.substr(4, 2), mon) )
	{
		throw base::Exception(ANAERR_ETLTIME_CONVERTION, "采集时间转换失败！无效的任务账期时间：%s [FILE:%s, LINE:%d]", TASK_CYCLE.c_str(), __FILE__, __LINE__);
	}

	// 账期日：默认为 1 号
	const base::SimpleTime ST_CYCLE(year, mon, 1, 0, 0, 0);
	if ( !ST_CYCLE.IsValid() )
	{
		throw base::Exception(ANAERR_ETLTIME_CONVERTION, "采集时间转换失败！无效的任务账期时间：%s [FILE:%s, LINE:%d]", TASK_CYCLE.c_str(), __FILE__, __LINE__);
	}

	// 取相差天数
	long day_diff = base::PubTime::DayDifference(ST_CYCLE, base::SimpleTime::Now());
	if ( day_diff < 0 )
	{
		throw base::Exception(ANAERR_ETLTIME_CONVERTION, "采集时间转换失败！无效的任务账期时间：%s [FILE:%s, LINE:%d]", TASK_CYCLE.c_str(), __FILE__, __LINE__);
	}

	std::string etl_time;
	base::PubStr::SetFormatString(etl_time, "day-%ld", day_diff);
	if ( !m_dbinfo.GenerateDayTime(etl_time) )
	{
		throw base::Exception(ANAERR_ETLTIME_CONVERTION, "采集时间转换失败！无法识别的采集时间表达式：%s [FILE:%s, LINE:%d]", etl_time.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Analyse_YC] 完成采集账期时间转换：[%s] -> [%s]", etl_time.c_str(), m_dbinfo.GetEtlDay().c_str());
}

void Analyse_YC::FetchUpdateFields_YW() throw(base::Exception)
{
	// 只有在进行详情表业务侧稽核时，才需获取更新字段
	if ( m_taskInfo.AnaRule.AnaType != AnalyseRule::ANATYPE_YCXQB_YW )
	{
		return;
	}

	FetchUpdateFieldsFromKpiCol(m_taskInfo.vecKpiDimCol, m_updFields.dim_upd_fields);
	m_pLog->Output("[Analyse_YC] 获得详情表(业务侧) DIM 更新字段总数：%lu", m_updFields.dim_upd_fields.size());

	FetchUpdateFieldsFromKpiCol(m_taskInfo.vecKpiValCol, m_updFields.val_upd_fields);
	m_pLog->Output("[Analyse_YC] 获得详情表(业务侧) VAL 更新字段总数：%lu", m_updFields.val_upd_fields.size());

	if ( m_taskInfo.vecKpiDimCol.size() < YCResult_XQB::S_PUBLIC_MEMBERS + 1 )
	{
		throw base::Exception(ANAERR_FETCH_UPD_FLD_YW, "获取业务侧更新字段失败！指标维度字段数为%lu，不足%d! [FILE:%s, LINE:%d]", m_taskInfo.vecKpiDimCol.size(), (YCResult_XQB::S_PUBLIC_MEMBERS+1), __FILE__, __LINE__);
	}

	// 取详情表字段名
	m_updFields.fld_billcyc = m_taskInfo.vecKpiDimCol[0].DBName;
	m_updFields.fld_city    = m_taskInfo.vecKpiDimCol[1].DBName;
	m_updFields.fld_batch   = m_taskInfo.vecKpiDimCol[YCResult_XQB::S_PUBLIC_MEMBERS-1].DBName;
	m_updFields.fld_dim     = m_taskInfo.vecKpiDimCol[YCResult_XQB::S_PUBLIC_MEMBERS].DBName;
}

void Analyse_YC::FetchUpdateFieldsFromKpiCol(std::vector<KpiColumn>& vec_kc, VEC_STRING& vec_up_fd)
{
	std::vector<KpiColumn> v_kc;
	VEC_STRING             v_upfd;

	// 从指标字段集提取业务更新字段
	const int VEC_SIZE = vec_kc.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		KpiColumn& ref_col = vec_kc[i];
		if ( KpiColumn::EWTYPE_YC_UPD_FD_YW == ref_col.ExpWay )
		{
			v_upfd.push_back(ref_col.DBName);

			m_pLog->Output("[Analyse_YC] 取得详情表(业务侧)更新字段：TYPE=[%s], "
																	"SEQ=[%d], "
																	"DB_NAME=[%s], "
																	"CN_NAME=[%s]", 
																	 (KpiColumn::CTYPE_DIM == ref_col.ColType ? "DIM" : "VAL"), 
																	 ref_col.ColSeq, 
																	 ref_col.DBName.c_str(), 
																	 ref_col.CNName.c_str());
		}
		else
		{
			v_kc.push_back(ref_col);
		}
	}

	v_kc.swap(vec_kc);
	v_upfd.swap(vec_up_fd);
}

void Analyse_YC::CreateStatFactor() throw(base::Exception)
{
	ReleaseStatFactor();

	const AnalyseRule::AnalyseType& ANA_TYPE = m_taskInfo.AnaRule.AnaType;
	if ( AnalyseRule::ANATYPE_YCHDB == ANA_TYPE )	// 核对表
	{
		m_pStatFactor = new YCStatFactor_HDB(m_dbinfo.GetEtlDay(), m_taskReq);
	}
	else	// 详情表
	{
		int item_size = 0;
		int val_size  = 0;
		GetFactorItemValSize(item_size, val_size);

		m_pStatFactor = new YCStatFactor_XQB(m_dbinfo.GetEtlDay(), m_taskReq, ANA_TYPE, item_size, val_size);
	}

	if ( NULL == m_pStatFactor )
	{
		throw base::Exception(ANAERR_CREATE_STATFACTOR, "Operator new YCStatFactor failed: 无法申请到内存空间！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}
}

void Analyse_YC::GetFactorItemValSize(int& item_size, int& val_size) const
{
	item_size = 0;
	val_size  = 0;

	CountKpiColumnItemValSize(m_taskInfo.vecKpiDimCol, item_size, val_size);
	CountKpiColumnItemValSize(m_taskInfo.vecKpiValCol, item_size, val_size);
}

void Analyse_YC::CountKpiColumnItemValSize(const std::vector<KpiColumn>& vec_column, int& item_size, int& val_size) const
{
	const int VEC_SIZE = vec_column.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		const KpiColumn& ref_col = vec_column[i];

		if ( KpiColumn::EWTYPE_YC_ITEM == ref_col.ExpWay )
		{
			++item_size;
		}
		else if ( KpiColumn::EWTYPE_YC_VALUE == ref_col.ExpWay )
		{
			++val_size;
		}
	}
}

void Analyse_YC::AnalyseSourceData() throw(base::Exception)
{
	//Analyse::AnalyseSourceData();

	GenerateNewBatch();

	ConvertStatFactor();

	GenerateResultData();

	// 生成了统计数据，再进行数据补全
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
	hd_batch.stat_date   = m_dbinfo.GetEtlDay().substr(0, 6);
	hd_batch.stat_city   = m_taskReq.task_city;
	hd_batch.stat_batch  = 0;
	m_pYCDB2->SelectHDBMaxBatch(m_dbinfo.target_table, hd_batch);
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
	xq_batch.city       = GetCity_XQB();
	xq_batch.type       = "0";			// 类型：0-固定项，1-浮动项
	xq_batch.busi_batch = 0;
	m_pYCDB2->SelectXQBMaxBatch(m_dbinfo.target_table, xq_batch);
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

	// 保留上一批次手工列数据
	KeepLastBatchManualData();

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
		StoreDetailResult_CW();
	}
	else	// 非详情表（财务侧）数据
	{
		m_pLog->Output("[Analyse_YC] 准备入库：业财报表稽核统计结果数据");
		m_pYCDB2->InsertResultData(m_dbinfo, m_v3HiveSrcData[0]);

		if ( AnalyseRule::ANATYPE_YCHDB == m_taskInfo.AnaRule.AnaType )	// 核对表
		{
			m_pLog->Output("[Analyse_YC] 准备入库：业财稽核差异汇总结果数据");
			StoreDiffSummaryResult();
		}
	}
}

void Analyse_YC::StoreDetailResult_CW()
{
	int item_size = 0;
	int val_size  = 0;
	GetFactorItemValSize(item_size, val_size);

	YCResult_XQB ycr(m_taskInfo.AnaRule.AnaType, item_size, val_size);
	std::vector<YCResult_XQB> vec_result;

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		VEC2_STRING& ref_vec2 = m_v3HiveSrcData[i];

		const int VEC2_SIZE = ref_vec2.size();
		for ( int j = 0; j < VEC2_SIZE; ++j )
		{
			if ( !ycr.Import(ref_vec2[j]) )
			{
				throw base::Exception(ANAERR_STORE_DETAIL_CW, "详情表（财务侧）结果数据导出失败！[INDEX:(%d, %d)] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", (i+1), (j+1), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}

			vec_result.push_back(ycr);
		}
	}

	m_pYCDB2->UpdateDetailCWResult(m_dbinfo, vec_result);
	m_pLog->Output("[Analyse_YC] Store detail (CW) result data size: %lu", vec_result.size());
}

void Analyse_YC::StoreDiffSummaryResult() throw(base::Exception)
{
	YCResult_HDB ycr;

	const int VEC2_SIZE = m_v2DiffSummary.size();
	for ( int i = 0; i < VEC2_SIZE; ++i )
	{
		if ( !ycr.Import(m_v2DiffSummary[i]) )
		{
			throw base::Exception(ANAERR_STORE_DIFF_SUMMARY, "差异汇总结果数据导出失败！[INDEX:%d] (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", (i+1), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}

		m_pYCDB2->UpdateInsertYCDIffSummary(m_dbinfo, ycr);
	}

	m_pLog->Output("[Analyse_YC] Store diff summary result data size: %d", VEC2_SIZE);
}

void Analyse_YC::KeepLastBatchManualData()
{
	// 只有在进行业务侧稽核时，才拷贝上一批次手工填列数据
	if ( m_taskInfo.AnaRule.AnaType != AnalyseRule::ANATYPE_YCXQB_YW )
	{
		return;
	}

	// 是否存在上一批次数据？
	// 当前批次小于2，没有上一批次数据
	if ( m_taskReq.task_batch < 2 )
	{
		m_pLog->Output("[Analyse_YC] No need to update the last batch manual data: NO last batch!");
		return;
	}

	if ( m_updFields.dim_upd_fields.empty() && m_updFields.val_upd_fields.empty() )
	{
		m_pLog->Output("[Analyse_YC] No need to update the last batch manual data: NO update fields!");
		return;
	}

	m_pLog->Output("[Analyse_YC] Update the last batch manual data ...");
	m_updFields.last_batch = m_taskReq.task_batch - 1;
	m_pYCDB2->UpdateLastBatchManualData(m_dbinfo.target_table, m_updFields, m_v3HiveSrcData[0]);
}

void Analyse_YC::RecordInformation()
{
	if ( AnalyseRule::ANATYPE_YCHDB == m_taskInfo.AnaRule.AnaType )	// 核对表
	{
		// 记录业财稽核日志
		RecordStatisticsLog();
	}

	// 登记报表状态："00"-待审核
	RecordReportState("00");

	// 登记流程记录日志
	RecordProcessLog();
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
	yc_srcinfo.period       = yc_log.stat_cycle.substr(0, 6);	// 取6位账期：YYYYMM
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

		m_pYCDB2->SelectYCSrcMaxBatch(yc_srcinfo);
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
	m_pYCDB2->InsertYCStatLog(yc_log);
}

std::string Analyse_YC::GetCity_XQB()
{
	// 业财详情表（省）稽核：地市指定为"GD"
	if ( AnalyseRule::ANATYPE_YCXQB_GD == m_taskInfo.AnaRule.AnaType )
	{
		return "GD";
	}
	else
	{
		return m_taskReq.task_city;
	}
}

std::string Analyse_YC::GetReportStateType()
{
	/// 类型：
	// 00-核对表
	// 01-地市账务业务详情表
	// 02-监控表
	// 03-地市财务对账详情表
	// 04-省财务详情表
	// 05-省财务监控表
	switch ( m_taskInfo.AnaRule.AnaType )
	{
	case AnalyseRule::ANATYPE_YCHDB:		// 业财核对表
		return "00";
	case AnalyseRule::ANATYPE_YCXQB_YW:		// 业财详情表（业务侧）
		return "01";
	case AnalyseRule::ANATYPE_YCXQB_CW:		// 业财详情表（财务侧）
		return "03";
	case AnalyseRule::ANATYPE_YCXQB_GD:		// 业财详情表（省财务侧）
		return "04";
	default:	// 未知类型
		return "";
	}
}

void Analyse_YC::RecordReportState(const std::string& state)
{
	if ( NULL == m_pStatFactor )
	{
		m_pLog->Output("[Analyse_YC] <WARNING> YCStatFactor 还没有生成，无法记录报表状态!!!");
		return;
	}

	m_reportState.report_id = m_pStatFactor->GetStatReport();
	m_reportState.bill_cyc  = m_dbinfo.GetEtlDay().substr(0, 6);
	m_reportState.city      = GetCity_XQB();
	m_reportState.status    = state;
	m_reportState.type      = GetReportStateType();
	m_reportState.actor     = m_taskReq.actor;

	m_pYCDB2->UpdateInsertReportState(m_reportState);
}

void Analyse_YC::RecordProcessLog()
{
	YCProcessLog proc_log;
	proc_log.report_id = m_reportState.report_id;
	proc_log.bill_cyc  = m_reportState.bill_cyc;
	proc_log.city      = m_reportState.city;
	proc_log.status    = m_reportState.status;
	proc_log.type      = m_reportState.type;
	proc_log.actor     = m_reportState.actor;
	proc_log.oper      = m_taskReq.oper;
	proc_log.version   = m_taskReq.task_batch;
	proc_log.uptime    = base::SimpleTime::Now().Time14();

	m_pYCDB2->InsertProcessLog(proc_log);
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

