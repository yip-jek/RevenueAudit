#pragma once

#include "basedb2.h"
#include "acqtaskinfo.h"

class CAcqDB2 : public base::BaseDB2
{
public:
	CAcqDB2(const std::string& db_name, const std::string& usr, const std::string& pw);
	virtual ~CAcqDB2();

	enum ADB_ERROR
	{
		ADBERR_SEL_ETL_RULE     = -2002001,			// 查询采集规则出错
		ADBERR_SEL_ETL_DIM      = -2002002,			// 查询采集维度规则出错
		ADBERR_SEL_ETL_VAL      = -2002003,			// 查询采集值规则出错
		ADBERR_SEL_DAT_SRC_TYPE = -2002004,			// 查询指标数据源类型出错
	};

public:
	// 设置指标规则表
	void SetTabKpiRule(const std::string& t_kpirule);

	// 设置采集规则表
	void SetTabEtlRule(const std::string& t_etlrule);

	// 设置采集维度规则表
	void SetTabEtlDim(const std::string& t_etldim);

	// 设置采集值规则表
	void SetTabEtlVal(const std::string& t_etlval);

	// 查询采集规则任务信息
	void SelectEtlTaskInfo(AcqTaskInfo& info) throw(base::Exception);

private:
	// 查询采集规则信息
	void SelectEtlRule(AcqTaskInfo& info) throw(base::Exception);

	// 查询采集维度规则信息
	void SelectEtlDim(const std::string& dim_id, std::vector<OneEtlDim>& vec_dim) throw(base::Exception);

	// 查询采集值规则信息
	void SelectEtlVal(const std::string& val_id, std::vector<OneEtlVal>& vec_val) throw(base::Exception);

	// 查询指标数据源类型
	void SelectKpiDataSrcType(AcqTaskInfo& info) throw(base::Exception);

private:
	// 数据库表名
	std::string	m_tabKpiRule;			// 指标规则表
	std::string	m_tabEtlRule;			// 采集规则表
	std::string	m_tabEtlDim;			// 采集维度规则表
	std::string	m_tabEtlVal;			// 采集值规则表
};

