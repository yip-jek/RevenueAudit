#pragma once

#include <baseframeapp.h>

// 采集模块
class Acquire : public base::BaseFrameApp
{   
public:
	Acquire();
	virtual ~Acquire();

	enum ACQ_ERROR
	{
		ACQERR_TASKINFO_ERROR    = -3000001,			// 任务信息异常
		ACQERR_KPIID_INVALID     = -3000002,			// 指标ID无效
		ACQERR_ETLID_INVALID     = -3000003,			// 采集规则ID无效
		ACQERR_HIVE_PORT_INVALID = -3000004				// Hive服务器端口无效
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

private:
	long		m_nKpiID;		// 指标ID
	long		m_nEtlID;		// 采集规则ID

	std::string m_sDBName;		// 数据库名称
	std::string m_sUsrName;		// 用户名
	std::string m_sPasswd;		// 密码

	std::string m_sHiveIP;		// Hive服务器IP地址
	int			m_nHivePort;	// Hive服务器端口
};

