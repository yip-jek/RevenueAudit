#include "ydalarmmanager.h"
#include "log.h"

YDAlarmManager::YDAlarmManager()
{
}

YDAlarmManager::~YDAlarmManager()
{
}

std::string YDAlarmManager::GetLogFilePrefix()
{
	return ("YDAlarmManager");
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

void YDAlarmManager::Init() throw(base::Exception)
{
	m_pLog->Output("[YDAlarmManager] Init OK.");
}

