#pragma once

#include "baseframeapp.h"
#include "anadbinfo.h"
#include "dimvaldiffer.h"
#include "uniformcodetransfer.h"

namespace base
{

class SQLTranslator;

}

class CAnaDB2;
class CAnaHive;
class AlarmEvent;

// 分析模块
class Analyse : public base::BaseFrameApp
{
public:
	Analyse();
	virtual ~Analyse();

	static const size_t COMPARE_HIVE_SRCDATA_SIZE = 4;		// 对比源数据个数

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
		ANAERR_SPLIT_HIVESQL_FAILED   = -3000013,			// 拆分可执行HIVE SQL失败
		ANAERR_GET_STATISTICS_FAILED  = -3000014,			// 生成一般统计HIVE SQL失败
		ANAERR_GET_REPORT_STAT_FAILED = -3000015,			// 生成报表统计HIVE SQL失败
		ANAERR_GET_STAT_BY_SET_FAILED = -3000016,			// 生成指定组的统计HIVE SQL失败
		ANAERR_ALARM_JUDGEMENT_FAILED = -3000017,			// 告警判断失败
		ANAERR_COMPARE_RESULT_DATA    = -3000018,			// 生成对比结果失败
		ANAERR_SRC_DATA_UNIFIED_CODE  = -3000019,			// 源数据的统一编码转换失败
		ANAERR_ANA_YCRA_DATA_FAILED   = -3000020,			// 分析业财稽核数据失败
		ANAERR_TRANS_YCFACTOR_FAILED  = -3000021,			// 业财稽核统计因子转换失败
		ANAERR_GENERATE_YCDATA_FAILED = -3000022,			// 生成业财稽核数据失败
		ANAERR_CAL_YCCMPLX_FAILED     = -3000023,			// 计算业财稽核组合因子失败
		ANAERR_GET_EXP_HIVESQL_FAILED = -3000024,			// 从分析表达式中生成HIVE SQL失败
		ANAERR_EXCHG_SQLMARK_FAILED   = -3000025,			// 标志转换失败
		ANAERR_GENE_DELTIME_FAILED    = -3000026,			// 生成数据删除的时间失败
	};

public:
	// 版本信息
	virtual const char* Version();

	// 载入参数配置信息
	virtual void LoadConfig() throw(base::Exception);

	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();

	// 初始化
	virtual void Init() throw(base::Exception);

	// 任务执行
	virtual void Run() throw(base::Exception);

	// 任务结束（资源回收）
	virtual void End(int err_code, const std::string& err_msg = std::string()) throw(base::Exception);

protected:
	// 释放 SQLTranslator 资源
	void ReleaseSQLTranslator();

	// 获取参数任务信息
	void GetParameterTaskInfo(const std::string& para) throw(base::Exception);

	// 获取后续参数任务信息
	virtual void GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception);

	// 设置任务信息
	void SetTaskInfo();

	// 获取任务信息
	virtual void FetchTaskInfo() throw(base::Exception);

	// 检查任务信息
	void CheckAnaTaskInfo() throw(base::Exception);

	// 检查后台表示方式类型
	void CheckExpWayType() throw(base::Exception);

	// 获取渠道、地市统一编码信息
	void FetchUniformCode() throw(base::Exception);

	// 进行数据分析
	void DoDataAnalyse() throw(base::Exception);

	// 解析分析规则，生成Hive取数逻辑
	virtual void AnalyseRules(std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 生成汇总对比类型的Hive SQL语句
	void GetSummaryCompareHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 生成明细对比类型的Hive SQL语句
	void GetDetailCompareHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 生成一般统计类型的Hive SQL语句
	void GetStatisticsHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 生成报表统计类型的Hive SQL语句
	void GetReportStatisticsHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 生成指定组的统计类型的Hive SQL语句
	void GetStatisticsHiveSQLBySet(std::vector<std::string>& vec_hivesql, bool union_all) throw(base::Exception);

	// 拆分可执行Hive SQL语句
	void SplitHiveSqlExpress(const std::string& exp_sqls, std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 确定所属数据组
	void DetermineDataGroup(const std::string& exp, int& first, int& second) throw(base::Exception);

	// 按类型生成目标表名称
	virtual void GenerateTableNameByType() throw(base::Exception);

	// 生成数据库信息
	void GetAnaDBInfo() throw(base::Exception);

	// 标志转换
	virtual void ExchangeSQLMark(std::string& sql) throw(base::Exception);

	// 获取Hive源数据
	void FetchHiveSource(std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 分析源数据，生成结果数据
	virtual void AnalyseSourceData() throw(base::Exception);

	// 源数据的地市与渠道的统一编码转换
	void SrcDataUnifiedCoding() throw(base::Exception);

	// 将Hive源数据转换为报表统计类型数据
	void TransSrcDataToReportStatData();

	//// 生成对比结果数据
	//void CompareResultData() throw(base::Exception);

	// 数据补全
	virtual void DataSupplement();

	// 收集维度取值
	void CollectDimVal();

	// 结果数据入库 [DB2]
	virtual void StoreResult() throw(base::Exception);

	// 删除旧数据
	virtual void RemoveOldResult(const AnaTaskInfo::ResultTableType& result_tabtype) throw(base::Exception);

	// 告警判断: 如果达到告警阀值，则生成告警
	virtual void AlarmJudgement() throw(base::Exception);

	// 波动告警
	void FluctuateAlarm(AlarmRule& alarm_rule) throw(base::Exception);

	// 对比告警
	void RatioAlarm(AlarmRule& alarm_rule) throw(base::Exception);

	// 处理告警事件
	void HandleAlarmEvent(std::vector<AlarmEvent>& vec_event) throw(base::Exception);

	// 更新维度取值范围
	virtual void UpdateDimValue();

	//// 添加分析条件
	//void AddAnalysisCondition(AnalyseRule& ana_rule, std::vector<std::string>& vec_sql);

protected:
	std::string m_sKpiID;				// 指标ID
	std::string m_sAnaID;				// 分析规则ID
	std::string m_sType;				// 分析类型

	std::string m_sDBName;				// 数据库名称
	std::string m_sUsrName;				// 用户名
	std::string m_sPasswd;				// 密码

	std::string m_zk_quorum;
	std::string m_krb5_conf;
	std::string m_usr_keytab;
	std::string m_principal;
	std::string m_jaas_conf;
	std::string m_sLoadJarPath;			// 依赖的 jar 包的路径

protected:
	CAnaDB2*    m_pAnaDB2;				// DB2数据库接口
	CAnaHive*   m_pAnaHive;				// Hive接口

protected:
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

protected:
	AnaTaskInfo          m_taskInfo;				// 指标任务信息
	DimValDiffer         m_DVDiffer;				// 用于维度取值范围的比较
	UniformCodeTransfer  m_UniCodeTransfer;			// 统一编码转换
	base::SQLTranslator* m_pSQLTranslator;			// 标志转换

protected:
	AnaDBInfo                                            m_dbinfo;				// 库表信息
	std::vector<std::vector<std::vector<std::string> > > m_v3HiveSrcData;		// 获取到的Hive源数据集
	std::vector<std::vector<std::string> >               m_v2ReportStatData;	// 报表统计类型的数据集
};

