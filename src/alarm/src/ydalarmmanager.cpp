#include "ydalarmmanager.h"
#include "log.h"
#include "simpletime.h"
#include "alarmerror.h"
#include "ydalarmdb.h"
#include "ydstruct.h"

YDAlarmManager::YDAlarmManager()
:m_pAlarmDB(NULL)
{
}

YDAlarmManager::~YDAlarmManager()
{
	ReleaseDBConnection();
}

std::string YDAlarmManager::GetLogFilePrefix()
{
	return ("YDAlarmManager");
}

void YDAlarmManager::Init() throw(base::Exception)
{
	InitDBConnection();

	m_pLog->Output("[YDAlarmManager] Init OK.");
}

void YDAlarmManager::LoadExtendedConfig() throw(base::Exception)
{
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_REQUEST");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_THRESHOLD");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_INFO");
	m_cfg.RegisterItem("TABLE", "TAB_SRC_DATA");

	m_cfg.ReadConfig();

	// 库表配置
	m_tabAlarmRequest   = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_REQUEST");
	m_tabAlarmThreshold = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_THRESHOLD");
	m_tabAlarmInfo      = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_INFO");
	m_tabSrcData        = m_cfg.GetCfgValue("TABLE", "TAB_SRC_DATA");

	m_pLog->Output("[YDAlarmManager] Load configuration OK.");
}

bool YDAlarmManager::ConfirmQuit()
{
	return true;
}

void YDAlarmManager::AlarmProcessing() throw(base::Exception)
{
	if ( ResponseAlarmRequest() )
	{
		DataAnalysis();
		GenerateAlarm();
	}
}

void YDAlarmManager::ReleaseDBConnection()
{
	if ( m_pAlarmDB != NULL )
	{
		delete m_pAlarmDB;
		m_pAlarmDB = NULL;
	}
}

void YDAlarmManager::InitDBConnection() throw(base::Exception)
{
	ReleaseDBConnection();

	m_pAlarmDB = new YDAlarmDB(m_dbinfo);
	if ( NULL == m_pAlarmDB )
	{
		throw base::Exception(ALMERR_INIT_DB_CONN, "Operator new YDAlarmDB failed: 无法申请到内存空间！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pAlarmDB->Connect();
	m_pAlarmDB->SetTabAlarmRequest(m_tabAlarmRequest);
	m_pAlarmDB->SetTabAlarmThreshold(m_tabAlarmThreshold);
	m_pAlarmDB->SetTabAlarmInfo(m_tabAlarmInfo);
	m_pAlarmDB->SetTabSrcData(m_tabSrcData);
}

bool YDAlarmManager::ResponseAlarmRequest()
{
	m_pAlarmDB->SelectAlarmRequest(m_vAlarmReq);

	const int VEC_SIZE = m_vAlarmReq.size();
	if ( VEC_SIZE > 0 )
	{
		m_pLog->Output("[YDAlarmManager] Get alarm requests: %d", VEC_SIZE);

		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			YDAlarmReq& ref_req = m_vAlarmReq[i];
			m_pLog->Output("[YDAlarmManager] Alarm request [%d]: SEQ=[%d], DATE=[%s], REGION=[%s]", (i+1), ref_req.seq, ref_req.alarm_date.c_str(), ref_req.region.c_str());
		}

		return true;
	}

	return false;
}

void YDAlarmManager::DataAnalysis()
{
}

void YDAlarmManager::GenerateAlarm()
{
}

