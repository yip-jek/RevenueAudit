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
		ADBERR_FETCH_ETL_DATA   = -2002004,			// 执行数据采集出错
		ADBERR_CHECK_SRC_TAB    = -2002005,			// 检查表是否存在出错
		ADBERR_SEL_ETL_SRC      = -2002006,			// 查询采集数据源出错
		ADBERR_SEL_YC_STATRULE  = -2002007,			// 查询业财稽核因子规则信息出错
		ADBERR_UPD_YC_TASK_REQ  = -2002008,			// （业财）更新任务请求表出错
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

	// 设置采集数据源表
	void SetTabEtlSrc(const std::string& t_etlsrc);

	// 设置统计因子规则表
	void SetTabYCStatRule(const std::string& t_statrule);

	// 查询采集规则任务信息
	void SelectEtlTaskInfo(AcqTaskInfo& info) throw(base::Exception);

	// 执行数据采集
	void FetchEtlData(const std::string& sql, int data_size, std::vector<std::vector<std::string> >& vec2_data) throw(base::Exception);

	// 表是否存在
	// 返回：true-表存在，false-表不存在
	bool CheckTableExisted(const std::string& tab_name) throw(base::Exception);

	// 查询业财稽核因子规则信息
	void SelectYCStatRule(const std::string& kpi_id, std::vector<YCInfo>& vec_ycinfo) throw(base::Exception);

// 业财稽核-任务调度
	// 设置任务请求表
	void SetTabYCTaskReq(const std::string& t_yc_taskreq);

	// 更新任务请求表
	void UpdateYCTaskReq(int seq, const std::string& state, const std::string& state_desc, const std::string& task_desc) throw(base::Exception);

private:
	// 查询采集规则信息
	void SelectEtlRule(AcqTaskInfo& info) throw(base::Exception);

	// 查询采集维度规则信息
	void SelectEtlDim(const std::string& dim_id, std::vector<OneEtlDim>& vec_dim) throw(base::Exception);

	// 查询采集值规则信息
	void SelectEtlVal(const std::string& val_id, std::vector<OneEtlVal>& vec_val) throw(base::Exception);

	// 查询采集数据源信息
	void SelectEtlSrc(const std::string& etlrule_id, std::map<int, EtlSrcInfo>& map_src) throw(base::Exception);

private:
	// 数据库表名
	std::string m_tabKpiRule;			// 指标规则表
	std::string m_tabEtlRule;			// 采集规则表
	std::string m_tabEtlDim;			// 采集维度规则表
	std::string m_tabEtlVal;			// 采集值规则表
	std::string m_tabEtlSrc;			// 采集数据源表
	std::string m_tabYCStatRule;		// 统计因子规则表（业财稽核）

// 业财稽核-任务调度
	std::string m_tabYCTaskReq;			// （业财）任务请求表
};

