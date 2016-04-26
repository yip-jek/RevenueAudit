#pragma once

#include "baseframeapp.h"

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

	// 更新维度取值范围
	void UpdateDimValue();

private:
	long		m_nKpiID;				// 指标ID
	long		m_nAnaID;				// 分析规则ID

	std::string m_sDBName;				// 数据库名称
	std::string m_sUsrName;				// 用户名
	std::string m_sPasswd;				// 密码

	std::string m_sHiveIP;				// Hive服务器IP地址
	int			m_nHivePort;			// Hive服务器端口

private:
	CAnaDB2*        m_pAnaDB2;			// DB2数据库接口
	CHiveThrift*    m_pCHive;			// Hive接口

private:
	std::string m_tabKpiRule;
	std::string m_tabKpiColumn;
	std::string m_tabDimValue;
	std::string m_tabEtlRule;
	std::string m_tabAnaRule;
	std::string m_tabAlarmRule;
};

