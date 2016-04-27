#include "analyse.h"
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "log.h"
#include "canadb2.h"
#include "chivethrift.h"


Analyse g_Analyse;


Analyse::Analyse()
:m_nKpiID(0)
,m_nAnaID(0)
,m_nHivePort(0)
,m_pAnaDB2(NULL)
,m_pCHive(NULL)
{
	g_pApp = &g_Analyse;
}

Analyse::~Analyse()
{
}

const char* Analyse::Version()
{
	return ("Analyse: Version 1.00.0023 released. Compiled at "__TIME__" on "__DATE__);
}

void Analyse::LoadConfig() throw(base::Exception)
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");
	m_cfg.RegisterItem("HIVE_SERVER", "IP_ADDRESS");
	m_cfg.RegisterItem("HIVE_SERVER", "PORT");

	m_cfg.RegisterItem("TABLE", "TAB_KPI_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_KPI_COLUMN");
	m_cfg.RegisterItem("TABLE", "TAB_DIM_VALUE");
	m_cfg.RegisterItem("TABLE", "TAB_ETL_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ANA_RULE");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_RULE");

	m_cfg.ReadConfig();

	// 数据库配置
	m_sDBName  = m_cfg.GetCfgValue("DATABASE", "DB_NAME");
	m_sUsrName = m_cfg.GetCfgValue("DATABASE", "USER_NAME");
	m_sPasswd  = m_cfg.GetCfgValue("DATABASE", "PASSWORD");

	// Hive服务器配置
	m_sHiveIP   = m_cfg.GetCfgValue("HIVE_SERVER", "IP_ADDRESS");
	m_nHivePort = (int)m_cfg.GetCfgLongVal("HIVE_SERVER", "PORT");
	if ( m_nHivePort <= 0 )
	{
		throw base::Exception(ANAERR_HIVE_PORT_INVALID, "Hive服务器端口无效! (port=%d) [FILE:%s, LINE:%d]", m_nHivePort, __FILE__, __LINE__);
	}

	// Tables
	m_tabKpiRule   = m_cfg.GetCfgValue("TABLE", "TAB_KPI_RULE");
	m_tabKpiColumn = m_cfg.GetCfgValue("TABLE", "TAB_KPI_COLUMN");
	m_tabDimValue  = m_cfg.GetCfgValue("TABLE", "TAB_DIM_VALUE");
	m_tabEtlRule   = m_cfg.GetCfgValue("TABLE", "TAB_ETL_RULE");
	m_tabAnaRule   = m_cfg.GetCfgValue("TABLE", "TAB_ANA_RULE");
	m_tabAlarmRule = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_RULE");

	m_pLog->Output("Load configuration OK.");
}

void Analyse::Init() throw(base::Exception)
{
	GetParameterTaskInfo();

	m_pDB2 = new CAnaDB2(m_sDBName, m_sUsrName, m_sPasswd);
	if ( NULL == m_pDB2 )
	{
		throw base::Exception(ANAERR_INIT_FAILED, "new CAnaDB2 failed: 无法申请到内存空间!");
	}
	m_pAnaDB2 = dynamic_cast<CAnaDB2*>(m_pDB2);

	m_pAnaDB2->SetTabKpiRule(m_tabKpiRule);
	m_pAnaDB2->SetTabKpiColumn(m_tabKpiColumn);
	m_pAnaDB2->SetTabDimValue(m_tabDimValue);
	m_pAnaDB2->SetTabEtlRule(m_tabEtlRule);
	m_pAnaDB2->SetTabAnaRule(m_tabAnaRule);
	m_pAnaDB2->SetTabAlarmRule(m_tabAlarmRule);

	m_pHiveThrift = new CHiveThrift(m_sHiveIP, m_nHivePort);
	if ( NULL == m_pHiveThrift )
	{
		throw base::Exception(ANAERR_INIT_FAILED, "new CHiveThrift failed: 无法申请到内存空间!");
	}
	m_pCHive = dynamic_cast<CHiveThrift*>(m_pHiveThrift);

	m_pCHive->Init();

	m_pLog->Output("Init OK.");
}

void Analyse::Run() throw(base::Exception)
{
	m_pAnaDB2->Connect();

	m_pCHive->Connect();

	AnaTaskInfo task_info;
	task_info.KpiID = m_nKpiID;

	m_pLog->Output("分析：查询分析任务规则信息 ...");

	m_pAnaDB2->SelectAnaTaskInfo(task_info);

	m_pLog->Output("分析：检查分析任务规则信息 ...");

	CheckAnaTaskInfo(task_info);

	//m_pCHive->Test("audit_bdzt_20160329");

	m_pLog->Output("分析：更新维度取值范围 ...");

	UpdateDimValue(task_info.KpiID);

	m_pCHive->Disconnect();

	m_pAnaDB2->Disconnect();
}

void Analyse::GetParameterTaskInfo() throw(base::Exception)
{
	// 格式：启动批号|指标ID|分析规则ID|...
	std::vector<std::string> vec_str;
	boost::split(vec_str, m_ppArgv[4], boost::is_any_of("|"));

	if ( vec_str.size() < 3 )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "参数任务信息异常(split size:%lu), 无法按格式拆分! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	try
	{
		m_nKpiID = boost::lexical_cast<long>(vec_str[1]);
	}
	catch ( boost::bad_lexical_cast& e )
	{
		throw base::Exception(ANAERR_KPIID_INVALID, "指标ID [%s] 无效! %s [FILE:%s, LINE:%d]", vec_str[1].c_str(), e.what(), __FILE__, __LINE__);
	}

	try
	{
		m_nAnaID = boost::lexical_cast<long>(vec_str[2]);
	}
	catch ( boost::bad_lexical_cast& e )
	{
		throw base::Exception(ANAERR_ANAID_INVALID, "分析规则ID [%s] 无效! %s [FILE:%s, LINE:%d]", vec_str[2].c_str(), e.what(), __FILE__, __LINE__);
	}
}

void Analyse::FetchHiveSource(AnaTaskInfo& info) throw(base::Exception)
{
	m_pCHive->
}

void Analyse::UpdateDimValue(int kpi_id)
{
	m_pAnaDB2->SelectDimValue(kpi_id, m_DVDiffer);

	m_pLog->Output("分析：从数据库中获取指标 (ID:%d) 的维度取值范围 size: %lu", kpi_id, m_DVDiffer.GetDBDimValSize());

	std::vector<DimVal> vec_diff_dv;
	m_DVDiffer.GetDimValDiff(vec_diff_dv);

	m_pAnaDB2->InsertNewDimValue(vec_diff_dv);

	m_pLog->Output("分析：更新维度取值范围成功! Update size: %lu", vec_diff_dv.size());
}

void Analyse::CheckAnaTaskInfo(AnaTaskInfo& info) throw(base::Exception)
{
	if ( info.vecKpiDimCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标维度信息! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( info.vecKpiValCol.empty() )
	{
		throw base::Exception(ANAERR_TASKINFO_INVALID, "没有指标值信息! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}
}

