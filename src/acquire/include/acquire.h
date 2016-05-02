#pragma once

#include "baseframeapp.h"
#include "acqtaskinfo.h"

class CAcqDB2;
class CHiveThrift;

// 采集模块
class Acquire : public base::BaseFrameApp
{
public:
	Acquire();
	virtual ~Acquire();

public:
	enum ACQ_ERROR
	{
		ACQERR_TASKINFO_ERROR       = -2000001,			// 任务信息异常
		ACQERR_KPIID_INVALID        = -2000002,			// 指标ID无效
		ACQERR_ETLID_INVALID        = -2000003,			// 采集规则ID无效
		ACQERR_HIVE_PORT_INVALID    = -2000004,			// Hive服务器端口无效
		ACQERR_INIT_FAILED          = -2000005,			// 初始化失败
		ACQERR_TASKINFO_INVALID     = -2000006,			// 任务信息无效
		ACQERR_TRANS_DATASRC_FAILED = -2000007,			// 源表转换失败
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
	void SetTaskInfo(AcqTaskInfo& info);

	// 获取任务信息
	void FetchTaskInfo(AcqTaskInfo& info) throw(base::Exception);

	// 检查采集任务信息
	void CheckTaskInfo(AcqTaskInfo& info) throw(base::Exception);

	// 进行数据采集
	void DoDataAcquisition(AcqTaskInfo& info) throw(base::Exception);

	// 分析采集规则，生成目标表字段
	void TaskInfo2TargetFields(AcqTaskInfo& info, std::vector<std::string>& vec_field) throw(base::Exception);

	// 分析采集任务规则，生成Hive_SQL
	void TaskInfo2HiveSql(AcqTaskInfo& info, std::vector<std::string>& vec_sql) throw(base::Exception);

	// 采集值对应源字段名转换
	std::string TransEtlValSrcName(const std::string& val_srcname);

	// 采集源数据表名日期转换
	std::string TransDataSrcDate(const std::string& time, const std::string& data_src) throw(base::Exception);

private:
	std::string	m_sKpiID;				// 指标ID
	std::string	m_sEtlID;				// 采集规则ID

	std::string m_sDBName;				// 数据库名称
	std::string m_sUsrName;				// 用户名
	std::string m_sPasswd;				// 密码

	std::string m_sHiveIP;				// Hive服务器IP地址
	int			m_nHivePort;			// Hive服务器端口

private:
	CAcqDB2*		m_pAcqDB2;			// DB2数据库接口
	CHiveThrift*	m_pCHive;			// Hive接口

private:
	// 数据库表名
	std::string	m_tabKpiRule;			// 指标规则表
	std::string	m_tabEtlRule;			// 采集规则表
	std::string	m_tabEtlDim;			// 采集维度规则表
	std::string	m_tabEtlVal;			// 采集值规则表
};

