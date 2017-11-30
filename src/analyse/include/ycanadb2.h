#pragma once

#include "canadb2.h"
#include "ycinfo.h"

class YCResult_XQB;

// 业财稽核-数据库类
class YCAnaDB2 : public CAnaDB2
{
public:
	YCAnaDB2(const std::string& db_name, const std::string& user, const std::string& pwd);
	virtual ~YCAnaDB2();

public:
	// 设置任务请求表
	void SetTabYCTaskReq(const std::string& t_yc_taskreq);

	// 设置统计因子规则表
	void SetTabYCStatRule(const std::string& t_statrule);

	// 设置稽核记录日志表
	void SetTabYCStatLog(const std::string& t_statlog);

	// 设置报表状态表
	void SetTabYCReportStat(const std::string& t_reportstat);

	// 设置流程记录日志表
	void SetTabYCProcessLog(const std::string& t_processlog);

	// 查询任务请求表信息
	void SelectYCTaskReq(YCTaskReq& task_req) throw(base::Exception);

	// 更新任务请求表
	void UpdateYCTaskReq(const YCTaskReq& t_req) throw(base::Exception);

	// 查询业财稽核因子规则信息
	void SelectYCStatRule(const std::string& kpi_id, std::vector<YCStatInfo>& vec_ycsi) throw(base::Exception);

	// 获取业财稽核地市核对表的最新批次
	void SelectHDBMaxBatch(const std::string& tab_hdb, YCHDBBatch& hd_batch) throw(base::Exception);

	// 获取业财稽核地市详情表的最新批次
	void SelectXQBMaxBatch(const std::string& tab_xqb, YCXQBBatch& xq_batch) throw(base::Exception);

	// 更新详情表（财务侧）结果数据
	void UpdateDetailCWResult(const AnaDBInfo& db_info, const std::vector<YCResult_XQB>& vec_result) throw(base::Exception);

	// 更新或插入差异汇总结果数据
	void UpdateInsertYCDIffSummary(const AnaDBInfo& db_info, const YCResult_HDB& ycr) throw(base::Exception);

	// 获取业财稽核数据源表的最新批次
	void SelectYCSrcMaxBatch(YCSrcInfo& yc_info) throw(base::Exception);

	// 入库业财稽核记录日志
	void InsertYCStatLog(const YCStatLog& stat_log) throw(base::Exception);

	// 更新或插入报表状态表
	void UpdateInsertReportState(const YCReportState& report_state) throw(base::Exception);

	// 插入流程记录日志
	void InsertProcessLog(const YCProcessLog& proc_log) throw(base::Exception);

	// 更新上一批次的手工填列数
	void UpdateLastBatchManualData(const std::string& tab, const UpdateFields_YW& upd_fld, std::vector<std::vector<std::string> >& v2_data) throw(base::Exception);

protected:
	std::string m_tabYCTaskReq;			// （任务调度）任务请求表
	std::string m_tabYCStatRule;		// 统计因子规则表
	std::string m_tabStatLog;			// 稽核记录日志表
	std::string m_tabReportStat;		// 报表状态表
	std::string m_tabProcessLog;		// 流程记录日志表
};

