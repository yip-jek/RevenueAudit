#pragma once

#include <map>
#include "basedb2.h"
#include "dimvaldiffer.h"
#include "ycinfo.h"

struct AnaDBInfo;
struct ChannelUniformCode;
struct CityUniformCode;
class AlarmEvent;

class CAnaDB2 : public base::BaseDB2
{
public:
	CAnaDB2(const std::string& db_name, const std::string& usr, const std::string& pw);
	virtual ~CAnaDB2();

	static const int DB_MAX_COMMIT = 20000;

public:
	// 设置指标规则表
	void SetTabKpiRule(const std::string& t_kpirule);

	// 设置指标字段表
	void SetTabKpiColumn(const std::string& t_kpicol);

	// 设置维度取值表
	void SetTabDimValue(const std::string& t_dimval);

	// 设置采集规则表
	void SetTabEtlRule(const std::string& t_etlrule);

	// 设置采集维度规则表
	void SetTabEtlDim(const std::string& t_etldim);

	// 设置采集值规则表
	void SetTabEtlVal(const std::string& t_etlval);

	// 设置分析规则表
	void SetTabAnaRule(const std::string& t_anarule);

	// 设置告警规则表
	void SetTabAlarmRule(const std::string& t_alarmrule);

	// 设置告警事件表
	void SetTabAlarmEvent(const std::string& t_alarmevent);

	// 设置渠道统一编码表
	void SetTabDictChannel(const std::string& t_dictchann);

	// 设置地市统一编码表
	void SetTabDictCity(const std::string& t_dictcity);

	// 设置任务日程日志表
	void SetTabTaskScheLog(const std::string& t_tslog);

	// 设置统计因子规则表
	void SetTabYCStatRule(const std::string& t_statrule);

	// 设置稽核记录日志表
	void SetTabYCStatLog(const std::string& t_statlog);

	// 查询分析规则任务信息
	void SelectAnaTaskInfo(AnaTaskInfo& info) throw(base::Exception);

	// 获取渠道统一编码数据
	void SelectChannelUniformCode(std::vector<ChannelUniformCode>& vec_channunicode) throw(base::Exception);

	// 获取地市统一编码数据
	void SelectCityUniformCode(std::vector<CityUniformCode>& vec_cityunicode) throw(base::Exception);

	// 查询维度取值表
	void SelectDimValue(const std::string& kpi_id, DimValDiffer& differ) throw(base::Exception);

	// 查询业财稽核因子规则信息
	void SelectYCStatRule(const std::string& kpi_id, std::vector<YCStatInfo>& vec_ycsi) throw(base::Exception);

	// 插入新的维度取值
	void InsertNewDimValue(std::vector<DimVal>& vec_dv) throw(base::Exception);

	// 插入结果数据
	void InsertResultData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception);

	// 统计已经存在的报表统计数据
	size_t SelectResultData(const std::string& tab_name, const std::string& condition) throw(base::Exception);

	// 删除已经存在的报表统计数据
	void DeleteResultData(AnaDBInfo& db_info, bool delete_all) throw(base::Exception);

	// 删除某个时间（段）的已存在的统计数据
	// 若 end_time <= 0，则表示某个指定时间（beg_time）
	// 否则，表示某个时间段：[beg_time, end_time]
	void DeleteTimeResultData(AnaDBInfo& db_info, int beg_time, int end_time) throw(base::Exception);

	// 插入报表统计数据
	void InsertReportStatData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_reportdata) throw(base::Exception);

	// 直接执行SQL
	void ExecuteSQL(const std::string& exe_sql) throw(base::Exception);

	// 获取目标数据
	void SelectTargetData(AnaDBInfo& db_info, const std::string& date, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception);

	// 获取最大告警事件 ID
	// 返回：true-成功获取，false-获取失败（无告警事件数据）
	bool SelectMaxAlarmEventID(int& max_event_id) throw(base::Exception);

	// 插入告警事件数据
	void InsertAlarmEvent(std::vector<AlarmEvent>& vec_event) throw(base::Exception);

	// 设置任务请求表
	void SetTabYCTaskReq(const std::string& t_yc_taskreq);

	// 查询任务请求表的地市信息
	void SelectYCTaskReqCity(int seq, std::string& city) throw(base::Exception);

	// 更新任务请求表
	void UpdateYCTaskReq(const YCTaskReq& t_req) throw(base::Exception);

	// 获取业财稽核统计结果表的最新批次
	void SelectStatResultMaxBatch(const std::string& tab_result, YCStatBatch& st_batch) throw(base::Exception);

	// 获取数据库序列值 (NEXTVAL)
	void SelectSequence(const std::string& seq_name, size_t size, std::vector<std::string>& vec_seq) throw(base::Exception);

	// 获取业财稽核数据源表的最新批次
	void SelectYCSrcMaxBatch(YCSrcInfo& yc_info) throw(base::Exception);

	// 入库业财稽核记录日志
	void InsertYCStatLog(const YCStatLog& stat_log) throw(base::Exception);

	// 更新任务日程日志表状态
	void UpdateTaskScheLogState(int log, const std::string& end_time, const std::string& state, const std::string& state_desc, const std::string& remark) throw(base::Exception);

	// (业财) 更新或插入差异汇总结果数据
	void UpdateInsertYCDIffSummary(const AnaDBInfo& db_info, const YCStatResult& ycsr) throw(base::Exception);

private:
	// 查询指标规则信息
	void SelectKpiRule(AnaTaskInfo& info) throw(base::Exception);

	// 查询指标字段表
	void SelectKpiColumn(AnaTaskInfo& info) throw(base::Exception);

	// 查询采集规则表
	void SelectEtlRule(OneEtlRule& one) throw(base::Exception);

	// 查询采集维度规则表
	void SelectEtlDim(const std::string& dim_id, std::vector<OneEtlDim>& vec_dim, std::vector<OneEtlDim>& vec_singledim) throw(base::Exception);

	// 查询采集值规则表
	void SelectEtlVal(const std::string& val_id, std::vector<OneEtlVal>& vec_val) throw(base::Exception);

	// 查询分析规则表
	void SelectAnaRule(AnalyseRule& ana) throw(base::Exception);

	// 查询告警规则表
	void SelectAlarmRule(AlarmRule& alarm) throw(base::Exception);

	// 查询对比结果描述（从维度取值表中获取）
	// 若没有获取到，则返回空集
	void SelectCompareResultDesc(const std::string& kpi_id, const std::string& comp_res_name, std::vector<std::string>& vec_comresdesc);

	// 获取对比结果字段名
	// 找不到，则返回空字符串
	std::string GetCompareResultName(std::vector<KpiColumn>& vec_kpival);

	// 清空结果表数据
	void AlterEmptyTable(const std::string& tab_name) throw(base::Exception);

	// 删除结果表数据
	void DeleteFromTable(const std::string& tab_name, const std::string& condition) throw(base::Exception);

	// 结果数据入库
	void ResultDataInsert(const std::string& db_sql, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception);

private:
	// 数据库表名
	std::string m_tabKpiRule;			// 指标规则表
	std::string m_tabKpiColumn;			// 指标字段表
	std::string m_tabDimValue;			// 维度取值表
	std::string m_tabEtlRule;			// 采集规则表
	std::string m_tabEtlDim;			// 采集维度规则表
	std::string m_tabEtlVal;			// 采集值规则表
	std::string m_tabAnaRule;			// 分析规则表
	std::string m_tabAlarmRule;			// 告警规则表
	std::string m_tabAlarmEvent;		// 告警事件表
	std::string m_tabDictChannel;		// 渠道统一编码表
	std::string m_tabDictCity;			// 地市统一编码表
	std::string m_tabTaskScheLog;		// 任务日程日志表

	std::string m_tabYCStatRule;		// （业财）统计因子规则表
	std::string m_tabStatLog;			// （业财）稽核记录日志表
	std::string m_tabYCTaskReq;			// （业财-任务调度）任务请求表
};

