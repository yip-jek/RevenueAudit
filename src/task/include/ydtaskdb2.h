#pragma once

#include "basedb2.h"
#include "ydstruct.h"

class YDTaskDB2 : public base::BaseDB2
{
public:
	// 数据库错误代码
	enum YDTASK_DB_ERROR
	{
		TDB_ERR_TAB_EXISTS     = -20000001,				// 检查表存在性失败
	};

public:
	YDTaskDB2(const DBInfo& db_info);
	virtual ~YDTaskDB2();

public:
	// 设置任务请求表
	void SetTabTaskSche(const std::string& tab_tasksche);

	// 设置指标规则表
	void SetTabKpiRule(const std::string& tab_kpirule);

	// 设置采集规则表
	void SetTabEtlRule(const std::string& tab_etlrule);

	// 表是否存在
	bool IsTableExists(const std::string& tab_name) throw(base::Exception);

private:
	std::string m_tabTaskSche;				// 任务日程表
	std::string m_tabKpiRule;				// 指标规则表
	std::string m_tabEtlRule;				// 采集规则表
};

