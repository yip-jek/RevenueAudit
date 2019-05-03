#pragma once

#include "basedb2.h"
#include "dimvaldiffer.h"

class AnaDBInfo;
struct ChannelUniformCode;
struct CityUniformCode;

// 分析：数据库类
class CAnaDB2 : public base::BaseDB2
{
public:
	CAnaDB2(const std::string& db_name, const std::string& user, const std::string& pwd);
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

	// 设置渠道统一编码表
	void SetTabDictChannel(const std::string& t_dictchann);

	// 设置地市统一编码表
	void SetTabDictCity(const std::string& t_dictcity);

	// 查询分析规则任务信息
	void SelectAnaTaskInfo(AnaTaskInfo& info);

	// 获取渠道统一编码数据
	void SelectChannelUniformCode(std::vector<ChannelUniformCode>& vec_channunicode);

	// 获取地市统一编码数据
	void SelectCityUniformCode(std::vector<CityUniformCode>& vec_cityunicode);

	// 查询维度取值表
	void SelectDimValue(const std::string& kpi_id, DimValDiffer& differ);

	// 插入新的维度取值
	void InsertNewDimValue(std::vector<DimVal>& vec_dv);

	// 插入结果数据
	void InsertResultData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_data);

	// 统计已经存在的报表统计数据
	size_t SelectResultData(const std::string& tab_name, const std::string& condition);

	// 删除已经存在的报表统计数据
	void DeleteResultData(AnaDBInfo& db_info, bool delete_all);

	// 删除某个时间（段）的已存在的统计数据
	// 若 end_time <= 0，则表示某个指定时间（beg_time）
	// 否则，表示某个时间段：[beg_time, end_time]
	void DeleteTimeResultData(AnaDBInfo& db_info, int beg_time, int end_time);

	// 插入报表统计数据
	void InsertReportStatData(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_reportdata);

	// 直接执行SQL
	void ExecuteSQL(const std::string& exe_sql);

	// 获取目标数据
	void SelectTargetData(AnaDBInfo& db_info, const std::string& date, std::vector<std::vector<std::string> >& vec2_data);

	// 获取数据库序列值 (NEXTVAL)
	void SelectSequence(const std::string& seq_name, size_t size, std::vector<std::string>& vec_seq);

protected:
	// 查询指标规则信息
	void SelectKpiRule(AnaTaskInfo& info);

	// 查询指标字段表
	void SelectKpiColumn(AnaTaskInfo& info);

	// 查询采集规则表
	void SelectEtlRule(OneEtlRule& one);

	// 查询采集维度规则表
	void SelectEtlDim(const std::string& dim_id, std::vector<OneEtlDim>& vec_dim, std::vector<OneEtlDim>& vec_singledim);

	// 查询采集值规则表
	void SelectEtlVal(const std::string& val_id, std::vector<OneEtlVal>& vec_val);

	// 查询分析规则表
	void SelectAnaRule(AnalyseRule& ana);

	// 查询对比结果描述（从维度取值表中获取）
	// 若没有获取到，则返回空集
	void SelectCompareResultDesc(const std::string& kpi_id, const std::string& comp_res_name, std::vector<std::string>& vec_comresdesc);

	// 获取对比结果字段名
	// 找不到，则返回空字符串
	std::string GetCompareResultName(std::vector<KpiColumn>& vec_kpival);

	// 清空结果表数据
	void AlterEmptyTable(const std::string& tab_name);

	// 删除结果表数据
	void DeleteFromTable(const std::string& tab_name, const std::string& condition);

	// 结果数据入库
	void ResultDataInsert(const std::string& db_sql, std::vector<std::vector<std::string> >& vec2_data);

protected:
	// 数据库表名
	std::string m_tabKpiRule;			// 指标规则表
	std::string m_tabKpiColumn;			// 指标字段表
	std::string m_tabDimValue;			// 维度取值表
	std::string m_tabEtlRule;			// 采集规则表
	std::string m_tabEtlDim;			// 采集维度规则表
	std::string m_tabEtlVal;			// 采集值规则表
	std::string m_tabAnaRule;			// 分析规则表
	std::string m_tabDictChannel;		// 渠道统一编码表
	std::string m_tabDictCity;			// 地市统一编码表
};

