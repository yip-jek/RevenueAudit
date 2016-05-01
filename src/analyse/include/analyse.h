#pragma once

#include "baseframeapp.h"
#include "dimvaldiffer.h"

class CAnaDB2;
class CHiveThrift;

// 分析模块
class Analyse : public base::BaseFrameApp
{
public:
	Analyse();
	virtual ~Analyse();

public:
	enum ANA_ERROR
	{
		ANAERR_TASKINFO_ERROR    = -3000001,			// 任务信息异常
		ANAERR_KPIID_INVALID     = -3000002,			// 指标ID无效
		ANAERR_ANAID_INVALID     = -3000003,			// 分析规则ID无效
		ANAERR_HIVE_PORT_INVALID = -3000004,			// Hive服务器端口无效
		ANAERR_INIT_FAILED       = -3000005,			// 初始化失败
		ANAERR_TASKINFO_INVALID  = -3000006,			// 任务信息无效
		ANAERR_ANA_RULE_FAILED   = -3000007,			// 解析分析规则失败
	};

public:
	// 版本信息
	virtual const char* Version();

	// 载入参数配置信息
	virtual void LoadConfig() throw(base::Exception);

	// 初始化
	virtual void Init() throw(base::Exception);

	// 任务执行
	virtual void Run() throw(base::Exception);

private:
	// 获取参数任务信息
	void GetParameterTaskInfo() throw(base::Exception);

	// 设置任务信息
	void SetTaskInfo(AnaTaskInfo& info);

	// 获取任务信息
	void FetchTaskInfo(AnaTaskInfo& info) throw(base::Exception);

	// 检查任务信息
	void CheckAnaTaskInfo(AnaTaskInfo& info) throw(base::Exception);

	// 进行数据分析
	void DoDataAnalyse(AnaTaskInfo& info) throw(base::Exception);

	// 解析分析规则，生成Hive取数逻辑
	void AnalyseRules(AnaTaskInfo& info, std::string& hive_sql) throw(base::Exception);

	// 获取Hive源数据
	void FetchHiveSource(const std::string& hive_sql) throw(base::Exception);

	// 分析源数据，生成结果数据
	void AnalyseSource();

	// 结果数据入库 [DB2]
	void StoreResult();

	// 告警判断: 如果达到告警阀值，则生成告警
	void AlarmJudgement();

	// 更新维度取值范围
	void UpdateDimValue(const std::string& kpi_id);

private:
	std::string	m_sKpiID;				// 指标ID
	std::string	m_sAnaID;				// 分析规则ID

	std::string m_sDBName;				// 数据库名称
	std::string m_sUsrName;				// 用户名
	std::string m_sPasswd;				// 密码

	std::string m_sHiveIP;				// Hive服务器IP地址
	int			m_nHivePort;			// Hive服务器端口

private:
	CAnaDB2*        m_pAnaDB2;			// DB2数据库接口
	CHiveThrift*    m_pCHive;			// Hive接口

private:
	// 数据库表名
	std::string	m_tabKpiRule;			// 指标规则表
	std::string m_tabKpiColumn;			// 指标字段表
	std::string m_tabDimValue;			// 维度取值表
	std::string	m_tabEtlRule;			// 采集规则表
	std::string m_tabAnaRule;			// 分析规则表
	std::string m_tabAlarmRule;			// 告警规则表
	std::string m_tabAlarmEvent;		// 告警事件表

private:
	DimValDiffer	m_DVDiffer;
};

