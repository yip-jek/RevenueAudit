#include "alarmmanager.h"
#include "log.h"
#include "gsignal.h"

AlarmManager::AlarmManager()
{
}

AlarmManager::~AlarmManager()
{
}

const char* AlarmManager::Version()
{
	return ("AlarmManager: Version 2.0001.20170426 released. Compiled at "__TIME__" on "__DATE__);
}

void AlarmManager::LoadConfig() throw(base::Exception)
{
	LoadBasicConfig();
	LoadExtendedConfig();
}

void AlarmManager::LoadBasicConfig() throw(base::Exception)
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");

	m_cfg.ReadConfig();

	// 数据库配置
	m_dbinfo.db_inst = m_cfg.GetCfgValue("DATABASE", "DB_NAME");
	m_dbinfo.db_user = m_cfg.GetCfgValue("DATABASE", "USER_NAME");
	m_dbinfo.db_pwd  = m_cfg.GetCfgValue("DATABASE", "PASSWORD");

	m_pLog->Output("[AlarmManager] Load configuration OK.");
}

void AlarmManager::Init() throw(base::Exception)
{
	m_pLog->Output("[AlarmManager] Init OK.");
}

void AlarmManager::Run() throw(base::Exception)
{
}

void AlarmManager::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
}

