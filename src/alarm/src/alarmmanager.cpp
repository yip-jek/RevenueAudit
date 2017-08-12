#include "alarmmanager.h"
#include "alarmerror.h"
#include "log.h"
#include "gsignal.h"
#include "sectimer.h"

AlarmManager::AlarmManager()
:m_timeInterval(0)
,m_AMState(AMS_BEGIN)
{
}

AlarmManager::~AlarmManager()
{
}

const char* AlarmManager::Version()
{
	return ("AlarmManager: Version 3.0.0.1 released. Compiled at " __TIME__ " on " __DATE__);
}

void AlarmManager::LoadConfig() throw(base::Exception)
{
	LoadBasicConfig();
	OutputBasicConfig();

	LoadExtendedConfig();
	OutputExtendedConfig();
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

	// 数据库配置
	m_dbinfo.db_inst = m_cfg.GetCfgValue("DATABASE", "DB_NAME");
	m_dbinfo.db_user = m_cfg.GetCfgValue("DATABASE", "USER_NAME");
	m_dbinfo.db_pwd  = m_cfg.GetCfgValue("DATABASE", "PASSWORD");

	m_pLog->Output("[AlarmManager] Load configuration OK.");
}

void AlarmManager::OutputBasicConfig()
{
	m_pLog->Output("================================================================================");
	m_pLog->Output("[AlarmManager] (BASIC_CONFIG) [SYS]->[TIME_INTERVAL] : [%d]", m_timeInterval);
	m_pLog->Output("[AlarmManager] (BASIC_CONFIG) [DATABASE]->[DB_NAME]  : [%s]", m_dbinfo.db_inst.c_str());
	m_pLog->Output("[AlarmManager] (BASIC_CONFIG) [DATABASE]->[USER_NAME]: [%s]", m_dbinfo.db_user.c_str());
	m_pLog->Output("[AlarmManager] (BASIC_CONFIG) [DATABASE]->[PASSWORD] : [%s]", m_dbinfo.db_pwd.c_str());
	m_pLog->Output("================================================================================");
}

void AlarmManager::Init() throw(base::Exception)
{
	if ( m_timeInterval <= 0 )
	{
		throw base::Exception(ALMERR_INIT, "The time interval is invalid: %d [FILE:%s, LINE:%d]", m_timeInterval, __FILE__, __LINE__);
	}

	m_pLog->Output("[AlarmManager] Init OK.");
}

void AlarmManager::Run() throw(base::Exception)
{
	// 设置退出信号捕获
	if ( !base::GSignal::Init(m_pLog) )
	{
		throw base::Exception(ALMERR_ALARMMGR_RUN, "Init setting signal failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 设置计时
	SecTimer st_wait(m_timeInterval);
	st_wait.Start();

	while ( Running() )
	{
		AlarmProcessing();

		// 等待下一次的任务执行
		st_wait.WaitForTimeUp();
	}
}

void AlarmManager::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	m_pLog->Output("[AlarmManager] END: MSG=[%s], CODE=[%d]", err_msg.c_str(), err_code);
}

bool AlarmManager::Running()
{
	switch ( m_AMState )
	{
	case AMS_BEGIN:			// 状态：开始
		// 改变状态为运行中
		m_AMState = AMS_RUNNING;
		break;
	case AMS_RUNNING:		// 状态：运行
		// 收到退出信号？
		if ( !base::GSignal::IsRunning() )
		{
			m_AMState = AMS_END;
		}
		break;
	case AMS_END:			// 状态：结束（收到退出信号）
		// 确认退出？
		if ( ConfirmQuit() )
		{
			m_AMState = AMS_QUIT;
		}
		break;
	case AMS_QUIT:			// 状态：退出
	default:				// 状态：未知
		// Do nothing !
		break;
	}

	return (AMS_QUIT != m_AMState);
}

