#pragma once

#include "baseframeapp.h"
#include "dimvaldiffer.h"

class CAnaDB2;
class CHiveThrift;
struct AnaDBInfo;

// 分析模块
class Analyse : public base::BaseFrameApp
{
public:
	Analyse();
	virtual ~Analyse();

public:
	enum ANA_ERROR
	{
		ANAERR_TASKINFO_ERROR         = -3000001,			// 任务信息异常
		ANAERR_KPIID_INVALID          = -3000002,			// 指标ID无效
		ANAERR_ANAID_INVALID          = -3000003,			// 分析规则ID无效
		ANAERR_HIVE_PORT_INVALID      = -3000004,			// Hive服务器端口无效
		ANAERR_INIT_FAILED            = -3000005,			// 初始化失败
		ANAERR_TASKINFO_INVALID       = -3000006,			// 任务信息无效
		ANAERR_ANA_RULE_FAILED        = -3000007,			// 解析分析规则失败
		ANAERR_GENERATE_TAB_FAILED    = -3000008,			// 生成目标表名失败
		ANAERR_GET_DBINFO_FAILED      = -3000009,			// 获取数据库信息失败
		ANAERR_GET_SUMMARY_FAILED     = -3000010,			// 生成汇总对比HIVE SQL失败
		ANAERR_GET_DETAIL_FAILED      = -3000011,			// 生成明细对比HIVE SQL失败
		ANAERR_DETERMINE_GROUP_FAILED = -3000012,			// 确定数据组失败
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
	void GetParameterTaskInfo(const std::string& para) throw(base::Exception);

	// 设置任务信息
	void SetTaskInfo(AnaTaskInfo& info);

	// 获取任务信息
	void FetchTaskInfo(AnaTaskInfo& info) throw(base::Exception);

	// 检查任务信息
	void CheckAnaTaskInfo(AnaTaskInfo& info) throw(base::Exception);

	// 进行数据分析
	void DoDataAnalyse(AnaTaskInfo& t_info) throw(base::Exception);

	// 解析分析规则，生成Hive取数逻辑
	void AnalyseRules(AnaTaskInfo& t_info, std::string& hive_sql, size_t& fields_num, AnaDBInfo& db_info) throw(base::Exception);

	// 生成汇总对比类型的Hive SQL语句
	std::string GetSummaryCompareHiveSQL(AnaTaskInfo& t_info) throw(base::Exception);

	// 生成明细对比类型的Hive SQL语句
	std::string GetDetailCompareHiveSQL(AnaTaskInfo& t_info) throw(base::Exception);

	// 确定所属数据组
	void DetermineDataGroup(const std::string& exp, int& first, int& second) throw(base::Exception);

	// 按类型生成目标表名称
	std::string GenerateTableNameByType(AnaTaskInfo& info) throw(base::Exception);

	// 统计从源数据插入到目标表的字段总数
	size_t GetTotalNumOfTargetFields(AnaTaskInfo& info);

	// 生成数据库信息
	void GetAnaDBInfo(AnaTaskInfo& t_info, AnaDBInfo& db_info) throw(base::Exception);

	// 获取Hive源数据
	void FetchHiveSource(const std::string& hive_sql, const size_t& total_num_of_fields, std::vector<std::vector<std::string> >& vv_fields) throw(base::Exception);

	// 分析源数据，生成结果数据
	void AnalyseSource(AnaTaskInfo& info, std::vector<std::vector<std::string> >& vec2_fields);

	// 收集维度取值
	void CollectDimVal(AnaTaskInfo& info, std::vector<std::vector<std::string> >& vec2_fields);

	// 结果数据入库 [DB2]
	void StoreResult(AnaDBInfo& db_info, std::vector<std::vector<std::string> >& vec2_fields) throw(base::Exception);

	// 告警判断: 如果达到告警阀值，则生成告警
	void AlarmJudgement(AnaTaskInfo& info, std::vector<std::vector<std::string> >& vec2_fields);

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
	std::string	m_tabEtlDim;			// 采集维度规则表
	std::string	m_tabEtlVal;			// 采集值规则表
	std::string m_tabAnaRule;			// 分析规则表
	std::string m_tabAlarmRule;			// 告警规则表
	std::string m_tabAlarmEvent;		// 告警事件表

private:
	DimValDiffer	m_DVDiffer;
};

