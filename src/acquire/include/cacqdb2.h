#pragma once

#include "basedb2.h"
#include "acqtaskinfo.h"

class CAcqDB2 : public base::BaseDB2
{
public:
	CAcqDB2(const std::string& db_name, const std::string& usr, const std::string& pw);
	virtual ~CAcqDB2();

public:
	// 设置指标规则表
	void SetTabKpiRule(const std::string& t_kpirule);

	// 设置采集规则表
	void SetTabEtlRule(const std::string& t_etlrule);

	// 设置采集维度规则表
	void SetTabEtlDim(const std::string& t_etldim);

	// 设置采集值规则表
	void SetTabEtlVal(const std::string& t_etlval);

	// 设置采集数据源表
	void SetTabEtlSrc(const std::string& t_etlsrc);

	// 设置任务日程日志表
	void SetTabTaskScheLog(const std::string& t_tslog);

	// 设置统计因子规则表
	void SetTabYCStatRule(const std::string& t_statrule);

	// 设置稽核报表状态表
	void SetTabReportState(const std::string& t_report);

	// 设置地市统一编码表
	void SetTabYCDictCity(const std::string& t_dictcity);

	// 查询采集规则任务信息
	void SelectEtlTaskInfo(AcqTaskInfo& info);

	// 执行数据采集
	void FetchEtlData(const std::string& sql, std::vector<std::vector<std::string> >& vec2_data);

	// 表是否存在
	// 返回：true-表存在，false-表不存在
	bool CheckTableExisted(const std::string& tab_name);

	// 查询业财稽核因子规则信息
	void SelectYCStatRule(const std::string& kpi_id, std::vector<YCInfo>& vec_ycinfo);

	// 获取业财稽核因子的SQL语句
	void SelectYCStatSQL(const std::string& src_sql, std::vector<std::string>& vec_sql);

	// 设置任务请求表
	void SetTabYCTaskReq(const std::string& t_yc_taskreq);

	// 查询任务请求表的信息
	void SelectYCTaskRequest(YCTaskRequest& task_req);

	// 查询任务地市的中文名称
	bool SelectYCTaskCityCN(const std::string& task_city, std::string& city_cn);

	// 查询指标规则类型
	std::string SelectKpiRuleType(const std::string& kpi_id);

	// 更新任务请求表
	void UpdateYCTaskReq(const YCTaskRequest& task_req);

	// 更新任务日程日志表状态
	void UpdateTaskScheLogState(int log, const std::string& end_time, const std::string& state, const std::string& state_desc, const std::string& remark);

	// 更新或插入报表状态表
	void UpdateInsertReportState(const YCReportState& report_state);

private:
	// 查询采集规则信息
	void SelectEtlRule(AcqTaskInfo& info);

	// 查询采集维度规则信息
	void SelectEtlDim(const std::string& dim_id, std::vector<OneEtlDim>& vec_dim);

	// 查询采集值规则信息
	void SelectEtlVal(const std::string& val_id, std::vector<OneEtlVal>& vec_val);

	// 查询采集数据源信息
	void SelectEtlSrc(const std::string& etlrule_id, std::map<int, EtlSrcInfo>& map_src);

private:
	// 数据库表名
	std::string m_tabKpiRule;			// 指标规则表
	std::string m_tabEtlRule;			// 采集规则表
	std::string m_tabEtlDim;			// 采集维度规则表
	std::string m_tabEtlVal;			// 采集值规则表
	std::string m_tabEtlSrc;			// 采集数据源表
	std::string m_tabTaskScheLog;		// 任务日程日志表

	std::string m_tabYCTaskReq;			// （业财）任务请求表
	std::string m_tabYCStatRule;		// （业财）统计因子规则表
	std::string m_tabReportStat;		// 稽核报表状态表
	std::string m_tabYCDictCity;		// （业财）地市统一编码表
};

