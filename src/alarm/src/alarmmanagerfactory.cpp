#include "alarmmanagerfactory.h"
#include "pubstr.h"
#include "alarmmanager.h"


AlarmManagerFactory gAlarmMgrFactory;

const char* const AlarmManagerFactory::S_MODE_YDJH = "ALARM_YDJH";			// 一点稽核

AlarmManagerFactory::AlarmManagerFactory()
{
	g_pFactory = &gAlarmMgrFactory;
}

AlarmManagerFactory::~AlarmManagerFactory()
{
}

base::BaseFrameApp* AlarmManagerFactory::CreateApp(const std::string& mode, std::string* pError)
{
	base::BaseFrameApp* pApp = NULL;

	const std::string ANA_MODE = base::PubStr::TrimUpperB(mode);
	if ( S_MODE_YDJH == ANA_MODE )
	{
		pApp = new AlarmManager();
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[ALARM_MGR_FACTORY] Create app failed - unsupport mode: %s\nOnly support mode: %s [FILE:%s, LINE:%d]", mode.c_str(), S_MODE_YDJH, __FILE__, __LINE__);
		}

		return NULL;
	}

	if ( NULL == pApp && pError != NULL )
	{
		base::PubStr::SetFormatString(*pError, "[ALARM_MGR_FACTORY] Operate new failed: 无法申请到内存空间! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	return pApp;
}

void AlarmManagerFactory::DestroyApp(base::BaseFrameApp** ppApp)
{
	if ( ppApp != NULL && *ppApp != NULL )
	{
		delete *ppApp;
		*ppApp = NULL;
	}
}

