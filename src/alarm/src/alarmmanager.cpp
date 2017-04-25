#include "alarmmanager.h"
#include "log.h"

AlarmManager::AlarmManager()
{
}

AlarmManager::~AlarmManager()
{
}

const char* AlarmManager::Version()
{
	return ("AlarmManager: Version 1.0001.20170425 released. Compiled at "__TIME__" on "__DATE__);
}

void AlarmManager::LoadConfig() throw(base::Exception)
{
	m_pLog->Output("[AlarmManager] Load configuration OK.");
}

std::string AlarmManager::GetLogFilePrefix()
{
	return ("AlarmManager");
}

void AlarmManager::Init() throw(base::Exception)
{
	m_pLog->Output("[AlarmManager] Init OK.");
}

void AlarmManager::Run() throw(base::Exception)
{
	m_pLog->Output("[AlarmManager] Running~~");
}

void AlarmManager::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	m_pLog->Output("[AlarmManager] END!");
}

