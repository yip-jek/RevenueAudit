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
	return ("AlarmManager: Version 2.0003.20170503 released. Compiled at "__TIME__" on "__DATE__);
}

void AlarmManager::LoadConfig() throw(base::Exception)
{
	LoadBasicConfig();
	LoadExtendedConfig();
}

void AlarmManager::LoadBasicConfig() throw(base::Exception)
{
	m_cfg.SetCfgFile(GetConfigFile());

	m_cfg.RegisterItem("SYS", "TIME_INTERVAL");
	m_cfg.RegisterItem("DATABASE", "DB_NAME");
	m_cfg.RegisterItem("DATABASE", "USER_NAME");
	m_cfg.RegisterItem("DATABASE", "PASSWORD");

	m_cfg.ReadConfig();

	// 处理时间间隔
	m_timeInterval = m_cfg.GetCfgLongVal("SYS", "TIME_INTERVAL");
	if ( m_timeInterval <= 0 )
	{
		throw base::Exception(ALMERR_LOAD_BASIC_CFG, "Invalid time interval seconds: %d [FILE:%s, LINE:%d]", m_timeInterval, __FILE__, __LINE__);
	}

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
	// 设置退出信号捕获
	if ( !base::GSignal::Init(m_pLog) )
	{
		throw base::Exception(ALMERR_ALARMMGR_RUN, "Init setting signal failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

}

void AlarmManager::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	m_pLog->Output("[AlarmManager] END: MSG=[%s], CODE={%d]", err_msg.c_str(), err_code);
}

