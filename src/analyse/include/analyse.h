#pragma once

#include "baseframeapp.h"
#include "anaerror.h"
#include "anadbinfo.h"
#include "dimvaldiffer.h"
#include "uniformcodetransfer.h"

namespace base
{

class SQLTranslator;

}

class CAnaDB2;
class CAnaHive;

// 分析模块
class Analyse : public base::BaseFrameApp
{
public:
	typedef std::vector<std::string>		VEC_STRING;
	typedef std::vector<VEC_STRING>			VEC2_STRING;
	typedef std::vector<VEC2_STRING>		VEC3_STRING;

public:
	Analyse();
	virtual ~Analyse();

	static const size_t COMPARE_HIVE_SRCDATA_SIZE = 4;		// 对比源数据个数

public:
	// 版本信息
	virtual const char* Version();

	// 载入参数配置信息
	virtual void LoadConfig() throw(base::Exception);

	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix() = 0;

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
	virtual void GetExtendParaTaskInfo(VEC_STRING& vec_str) throw(base::Exception) = 0;

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
	virtual void AnalyseRules(VEC_STRING& vec_hivesql) throw(base::Exception);

	// 生成汇总对比类型的Hive SQL语句
	void GetSummaryCompareHiveSQL(VEC_STRING& vec_hivesql) throw(base::Exception);

	// 生成明细对比类型的Hive SQL语句
	void GetDetailCompareHiveSQL(VEC_STRING& vec_hivesql) throw(base::Exception);

	// 生成一般统计类型的Hive SQL语句
	void GetStatisticsHiveSQL(VEC_STRING& vec_hivesql) throw(base::Exception);

	// 生成报表统计类型的Hive SQL语句
	void GetReportStatisticsHiveSQL(VEC_STRING& vec_hivesql) throw(base::Exception);

	// 生成指定组的统计类型的Hive SQL语句
	void GetStatisticsHiveSQLBySet(VEC_STRING& vec_hivesql, bool union_all) throw(base::Exception);

	// 拆分可执行Hive SQL语句
	void SplitHiveSqlExpress(const std::string& exp_sqls, VEC_STRING& vec_hivesql) throw(base::Exception);

	// 确定所属数据组
	void DetermineDataGroup(const std::string& exp, int& first, int& second) throw(base::Exception);

	// 按类型生成目标表名称
	virtual void GenerateTableNameByType() throw(base::Exception);

	// 生成数据库信息
	virtual void GetAnaDBInfo() throw(base::Exception);

	// 标志转换
	virtual void ExchangeSQLMark(std::string& sql) throw(base::Exception);

	// 获取Hive源数据
	void FetchHiveSource(VEC_STRING& vec_hivesql) throw(base::Exception);

	// 分析源数据，生成结果数据
	virtual void AnalyseSourceData() throw(base::Exception);

	// 源数据的地市与渠道的统一编码转换
	void SrcDataUnifiedCoding() throw(base::Exception);

	// 生成报表数据
	void GenerateReportStatData();

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

	// 更新维度取值范围
	virtual void UpdateDimValue();

	//// 添加分析条件
	//void AddAnalysisCondition(AnalyseRule& ana_rule, VEC_STRING& vec_sql);

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
	std::string m_tabDictChannel;		// 渠道统一编码表
	std::string m_tabDictCity;			// 地市统一编码表

protected:
	AnaTaskInfo          m_taskInfo;				// 指标任务信息
	DimValDiffer         m_DVDiffer;				// 用于维度取值范围的比较
	UniformCodeTransfer  m_UniCodeTransfer;			// 统一编码转换
	base::SQLTranslator* m_pSQLTranslator;			// 标志转换

protected:
	AnaDBInfo   m_dbinfo;							// 库表信息
	VEC3_STRING m_v3HiveSrcData;					// 获取到的Hive源数据集
	VEC2_STRING m_v2ReportStatData;					// 报表统计类型的数据集
};

