#pragma once

#include "baseframeapp.h"
#include "acqtaskinfo.h"
#include "hdfsconnector.h"
#include "pubtime.h"

class CAcqDB2;
class CAcqHive;

// 采集模块
class Acquire : public base::BaseFrameApp
{
public:
	Acquire();
	virtual ~Acquire();

public:
	enum ACQ_ERROR
	{
		ACQERR_TASKINFO_ERROR          = -2000001,			// 任务信息异常
		ACQERR_KPIID_INVALID           = -2000002,			// 指标ID无效
		ACQERR_ETLID_INVALID           = -2000003,			// 采集规则ID无效
		ACQERR_HIVE_PORT_INVALID       = -2000004,			// Hive服务器端口无效
		ACQERR_INIT_FAILED             = -2000005,			// 初始化失败
		ACQERR_TASKINFO_INVALID        = -2000006,			// 任务信息无效
		ACQERR_TRANS_DATASRC_FAILED    = -2000007,			// 源表转换失败
		ACQERR_OUTER_JOIN_FAILED       = -2000008,			// 外连条件下生成Hive SQL失败
		ACQERR_DATA_ACQ_FAILED         = -2000009,			// 数据采集失败
		ACQERR_HDFS_PORT_INVALID       = -2000010,			// HDFS端口无效
		ACQERR_OUTPUT_HDFS_FILE_FAILED = -2000011,			// 输出到HDFS文件失败
		ACQERR_LOAD_HIVE_FAILED        = -2000012,			// 载入数据到HIVE失败
		ACQERR_CHECK_SRC_TAB_FAILED    = -2000013,			// 检查源表失败
		ACQERR_GEN_ETL_DATE_FAILED     = -2000014,			// 生成采集时间失败
		ACQERR_EXCHANGE_SQLMARK_FAILED = -2000015,			// 标记转换失败
		ACQERR_YC_STATRULE_SQL_FAILED  = -2000016,			// 生成业财稽核SQL失败
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
	// 获取参数任务信息
	void GetParameterTaskInfo(const std::string& para) throw(base::Exception);

	// 设置任务信息
	void SetTaskInfo();

	// 获取任务信息
	void FetchTaskInfo() throw(base::Exception);

	// 检查采集任务信息
	void CheckTaskInfo() throw(base::Exception);

	// 获取业财稽核的因子规则
	void GetYCRAStatRule() throw(base::Exception);

	// 进行数据采集
	void DoDataAcquisition() throw(base::Exception);

	// 生成采集时间
	void GenerateEtlDate(const std::string& date_fmt) throw(base::Exception);

	// HIVE数据采集
	void HiveDataAcquisition() throw(base::Exception);

	// DB2数据采集
	void DB2DataAcquisition() throw(base::Exception);

	// 检查源表是否存在
	void CheckSourceTable(bool hive) throw(base::Exception);

	// 载入HDFS配置
	void LoadHdfsConfig() throw(base::Exception);

	// 生成hdfs临时文件名
	std::string GeneralHdfsFileName();

	// 将DB2数据写到HDFS文件
	// 返回：文件存在于HDFS的详细路径
	std::string DB2DataOutputHdfsFile(std::vector<std::vector<std::string> >& vec2_data, hdfsFS& hd_fs, const std::string& hdfs_file) throw(base::Exception);

	// 从HDFS文件load数据到Hive
	void LoadHdfsFile2Hive(const std::string& target_tab, const std::string& hdfs_file) throw(base::Exception);

	// 重建Hive目标表
	// 返回：目标表的字段数
	int RebuildHiveTable() throw(base::Exception);

	// 分析采集规则，生成目标表字段
	void TaskInfo2TargetFields(std::vector<std::string>& vec_field) throw(base::Exception);

	// 分析采集任务规则，生成采集SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	void TaskInfo2Sql(std::vector<std::string>& vec_sql, bool hive) throw(base::Exception);

	// 分析统计因子规则，生成业财稽核SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	void YCStatRule2Sql(std::vector<std::string>& vec_sql, bool hive) throw(base::Exception);

	// 外连条件下：分析采集规则，生成采集SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	// 参数 with_cond：true-带 SQL 条件，false-不带 SQL 条件(默认值)
	void OuterJoin2Sql(std::vector<std::string>& vec_sql, bool hive, bool with_cond = false) throw(base::Exception);

	// 不带条件或者直接条件下：分析采集规则，生成采集SQL
	// 参数 hive：true-数据来源于 HIVE，false-数据来源于 DB2
	void NoneOrStraight2Sql(std::vector<std::string>& vec_sql, bool hive);

	// 获取 SQL 条件
	std::string GetSQLCondition(const std::string& cond);

	// 标识转换
	void ExchangeSQLMark(std::string& sql) throw(base::Exception);

	// 采集源数据表名日期转换
	std::string TransSourceDate(const std::string& src_tabname) throw(base::Exception);

protected:
	bool        m_bRetainResult;		// 是否保留结果数据
	std::string m_sInsertMode;			// 插入模式（直接插入 或者 覆盖重复数据）
	std::string	m_sKpiID;				// 指标ID
	std::string	m_sEtlID;				// 采集规则ID

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

	std::string m_sHdfsHost;			// HDFS的主机信息
	std::string m_sHdfsTmpPath;			// HDFS的临时目录
	int			m_nHdfsPort;			// HDFS的端口
	int			m_seqHdfsFile;			// HDFS文件的流水号

protected:
	CAcqDB2*	m_pAcqDB2;				// DB2数据库接口
	CAcqHive*	m_pAcqHive;				// Hive接口

protected:
	AcqTaskInfo					m_taskInfo;			// 采集任务信息
	base::PubTime::DATE_TYPE	m_acqDateType;		// 采集时间类型
	std::string					m_acqDate;			// 采集时间

protected:
	// 数据库表名
	std::string	m_tabKpiRule;			// 指标规则表
	std::string	m_tabEtlRule;			// 采集规则表
	std::string	m_tabEtlDim;			// 采集维度规则表
	std::string	m_tabEtlVal;			// 采集值规则表
	std::string	m_tabEtlSrc;			// 采集数据源表

// 业财稽核-任务调度
#ifdef _YCRA_TASK
	int         m_ycSeqID;				// 任务流水号
	std::string m_tabYCTaskReq;			// （业财）任务请求表
#endif

protected:
	bool				m_isYCRA;			// 是否为业财稽核
	std::vector<YCInfo>	m_vecYCInfo;		// 业财稽核因子规则信息
};

