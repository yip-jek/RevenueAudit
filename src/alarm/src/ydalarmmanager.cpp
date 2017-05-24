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

	m_cfg.ReadConfig();

	// 库表配置
	m_tabAlarmRequest   = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_REQUEST");
	m_tabAlarmThreshold = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_THRESHOLD");
	m_tabAlarmInfo      = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_INFO");

	m_pLog->Output("[YDAlarmManager] Load configuration OK.");
}

bool YDAlarmManager::ConfirmQuit()
{
	return true;
}

void YDAlarmManager::AlarmProcessing() throw(base::Exception)
{
	ResponseAlarmRequest();
	DataAnalysis();
	GenerateAlarm();
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
}

void YDAlarmManager::ResponseAlarmRequest()
{
	m_pAlarmDB->SelectAlarmRequest(m_vAlarmReq);

	const int VEC_SIZE = m_vAlarmReq.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		if ( 0 == i )
		{
			m_pLog->Output("[YDAlarmManager] Alarm request: %d", VEC_SIZE);
		}

		YDAlarmReq& ref_req = m_vAlarmReq[i];
		m_pLog->Output("[YDAlarmManager] >>>>>>>>>>>> [%d]", (i+1));
		m_pLog->Output("[YDAlarmManager] SEQ         :[%d]", ref_req.seq);
		m_pLog->Output("[YDAlarmManager] ALARM_DATE  :[%s]", ref_req.alarm_date.c_str());
		m_pLog->Output("[YDAlarmManager] REGION      :[%s]", ref_req.region.c_str());
		m_pLog->Output("[YDAlarmManager] CHANNEL_TYPE:[%s]", ref_req.channel_type.c_str());
		m_pLog->Output("[YDAlarmManager] CHANNEL_NAME:[%s]", ref_req.channel_name.c_str());
		m_pLog->Output("[YDAlarmManager] BUSI_TYPE   :[%s]", ref_req.busi_type.c_str());
		m_pLog->Output("[YDAlarmManager] PAY_TYPE    :[%s]", ref_req.pay_type.c_str());
		m_pLog->Output("[YDAlarmManager] ALARM_STATUS:[%s]", ref_req.GetReqStatus().c_str());
		m_pLog->Output("[YDAlarmManager] REQ_TIME    :[%s]", ref_req.req_time.c_str());
		m_pLog->Output("[YDAlarmManager] FINISH_TIME :[%s]", ref_req.finish_time.c_str());
		m_pLog->Output("[YDAlarmManager] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

		ref_req.SetReqStatus("1");
		ref_req.finish_time = base::SimpleTime::Now().Time14();
		m_pAlarmDB->UpdateAlarmRequest(ref_req);
	}
}

void YDAlarmManager::DataAnalysis()
{
}

void YDAlarmManager::GenerateAlarm()
{
}

