#pragma once

#include "basedb2.h"
//#include "acqtaskinfo.h"

class CAnaDB2 : public base::BaseDB2
{
public:
	CAnaDB2(const std::string& db_name, const std::string& usr, const std::string& pw);
	virtual ~CAnaDB2();

	enum ADB_ERROR
	{
		ADBERR_SEL_ETL_RULE = -3002001,			// 查询采集规则出错
		ADBERR_SEL_ETL_DIM  = -3002002,			// 查询采集维度规则出错
		ADBERR_SEL_ETL_VAL  = -3002003,			// 查询采集值规则出错
	};

public:
	// 设置指标规则表
	void SetTabKpiRule(const std::string& t_kpirule);

	// 设置指标字段表
	void SetTabKpiColumn(const std::string& t_kpicol);

	// 设置维度取值表
	void SetTabDimValue(const std::string& t_dimval);

	// 设置采集规则表
	void SetTabEtlRule(const std::string& t_etlrule);

	// 设置分析规则表
	void SetTabAnaRule(const std::string& t_anarule);

	// 设置告警规则表
	void SetTabAlarmRule(const std::string& t_alarmrule);

	//// 查询采集规则任务信息
	//void SelectEtlTaskInfo(AcqTaskInfo& info) throw(base::Exception);

private:
	//// 查询采集规则信息
	//void SelectEtlRule(AcqTaskInfo& info) throw(base::Exception);

	//// 查询采集维度规则信息
	//void SelectEtlDim(int dim_id, std::vector<OneEtlDim>& vec_dim) throw(base::Exception);

	//// 查询采集值规则信息
	//void SelectEtlVal(int val_id, std::vector<OneEtlVal>& vec_val) throw(base::Exception);

private:
	// 数据库表名
	std::string	m_tabKpiRule;			// 指标规则表
	std::string m_tabKpiColumn;			// 指标字段表
	std::string m_tabDimValue;			// 维度取值表
	std::string	m_tabEtlRule;			// 采集规则表
	std::string m_tabAnaRule;			// 分析规则表
	std::string m_tabAlarmRule;			// 告警规则表
};

