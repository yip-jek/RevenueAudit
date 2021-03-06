#pragma once

#include "baseframeapp.h"
#include "acqerror.h"
#include "acqtaskinfo.h"
#include "hdfsconnector.h"
#include "pubtime.h"

namespace base
{

class SQLTranslator;

}

class CAcqDB2;
class CAcqHive;

// 采集模块
class Acquire : public base::BaseFrameApp
{
public:
	Acquire();
	virtual ~Acquire();

public:
	// 版本信息
	virtual const char* Version();

	// 载入参数配置信息
	virtual void LoadConfig();

	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix() = 0;

	// 初始化
	virtual void Init();

	// 任务执行
	virtual void Run();

	// 任务结束（资源回收）
	virtual void End(int err_code, const std::string& err_msg = std::string());

protected:
	// 释放 SQLTranslator 资源
	void SQLTransRelease();

	// 获取参数任务信息
	void GetParameterTaskInfo(const std::string& para);

	// 获取后续参数任务信息
	virtual void GetExtendParaTaskInfo(std::vector<std::string>& vec_str) = 0;

	// 设置任务信息
	void SetTaskInfo();

	// 获取任务信息
	virtual void FetchTaskInfo();

	// 检查采集任务信息
	virtual void CheckTaskInfo();

	// 进行数据采集
	virtual void DoDataAcquisition();

	// 生成采集时间
	virtual void GenerateEtlDate(const std::string& date_fmt);

	// HIVE数据采集
	void HiveDataAcquisition();

	// DB2数据采集
	void DB2DataAcquisition();

	// 检查源表是否存在
	virtual void CheckSourceTable(bool hive);

	// 载入HDFS配置
	void LoadHdfsConfig();

	// 生成hdfs临时文件名
	virtual std::string GeneralHdfsFileName();

	// 将DB2数据写到HDFS文件
	// 返回：文件存在于HDFS的详细路径
	std::string DB2DataOutputHdfsFile(std::vector<std::vector<std::string> >& vec2_data, hdfsFS& hd_fs, const std::string& hdfs_file);

	// 从HDFS文件load数据到Hive
	void LoadHdfsFile2Hive(const std::string& target_tab, const std::string& hdfs_file);

	// 重建Hive目标表
	// 返回：目标表的字段数
	virtual int RebuildHiveTable();

	// 分析采集规则，生成目标表字段
	void TaskInfo2TargetFields(std::vector<std::string>& vec_field);

	// 分析采集任务规则，生成采集SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	virtual void TaskInfo2Sql(std::vector<std::string>& vec_sql, bool hive);

	// 外连条件下：分析采集规则，生成采集SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	// 参数 with_cond：true-带 SQL 条件，false-不带 SQL 条件(默认值)
	void OuterJoin2Sql(std::vector<std::string>& vec_sql, bool hive, bool with_cond = false);

	// 不带条件或者直接条件下：分析采集规则，生成采集SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	void NoneOrStraight2Sql(std::vector<std::string>& vec_sql, bool hive);

	// 获取 SQL 条件
	std::string GetSQLCondition(const std::string& cond);

	// 标识转换
	void ExchangeSQLMark(std::string& sql);

	// 采集源数据表名日期转换
	std::string TransSourceDate(const std::string& src_tabname);

protected:
	bool        m_bRetainResult;		// 是否保留结果数据
	std::string m_sInsertMode;			// 插入模式（直接插入 或者 覆盖重复数据）
	std::string m_sKpiID;				// 指标ID
	std::string m_sEtlID;				// 采集规则ID
	std::string m_sType;				// 采集类型

	std::string m_sDBName;				// 数据库名称
	std::string m_sUsrName;				// 用户名
	std::string m_sPasswd;				// 密码

	std::string m_zk_quorum;
	std::string m_krb5_conf;
	std::string m_usr_keytab;
	std::string m_principal;
	std::string m_jaas_conf;
	std::string m_sLoadJarPath;			// 依赖的 jar 包路径
	std::string m_hiveTabLocation;		// HIVE 库表的路径

	std::string m_sHdfsHostInfo;		// HDFS的主机组合信息
	std::string m_sHdfsHost;			// HDFS的主机信息
	std::string m_sHdfsTmpPath;			// HDFS的临时目录
	int         m_nHdfsPort;			// HDFS的端口
	int         m_seqHdfsFile;			// HDFS文件的流水号

protected:
	CAcqDB2*    m_pAcqDB2;				// DB2数据库接口
	CAcqHive*   m_pAcqHive;				// Hive接口

protected:
	AcqTaskInfo              m_taskInfo;			// 采集任务信息
	base::PubTime::DATE_TYPE m_acqDateType;			// 采集时间类型
	std::string              m_acqDate;				// 采集时间
	base::SQLTranslator*     m_pSQLTrans;			// SQL语句转换

protected:
	// 数据库表名
	std::string m_tabKpiRule;			// 指标规则表
	std::string m_tabEtlRule;			// 采集规则表
	std::string m_tabEtlDim;			// 采集维度规则表
	std::string m_tabEtlVal;			// 采集值规则表
	std::string m_tabEtlSrc;			// 采集数据源表
};

