#include "factory.h"
#include "pubstr.h"
#include "baseframeapp.h"

base::Factory* g_pFactory = NULL;

namespace base
{

const char* const Factory::S_VAR_DEBUG   = "DEBUG";					// 测试版本
const char* const Factory::S_VAR_RELEASE = "RELEASE";				// 发布版本

Factory::Factory()
{
}

Factory::~Factory()
{
}

BaseFrameApp* Factory::Create(const std::string& mode, const std::string& var, std::string* pError)
{
	bool is_test = false;
	BaseFrameApp* pApp = NULL;

	const std::string VARIANT = base::PubStr::TrimUpperB(var);
	if ( S_VAR_DEBUG == VARIANT )
	{
		is_test = true;
	}
	else if ( S_VAR_RELEASE == VARIANT )
	{
		is_test = false;
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[FACTORY] Create app failed - unsupport variant: %s\nOnly support variant: %s %s [FILE:%s, LINE:%d]", var.c_str(), S_VAR_DEBUG, S_VAR_RELEASE, __FILE__, __LINE__);
		}

		return NULL;
	}

	pApp = CreateApp(mode, pError);
	if ( pApp != NULL )
	{
		pApp->SetTestFlag(is_test);
		m_listApp.push_back(pApp);
	}

	return pApp;
}

void Factory::Release()
{
	if ( !m_listApp.empty() )
	{
		for ( std::list<BaseFrameApp*>::iterator it = m_listApp.begin(); it != m_listApp.end(); ++it )
		{
			DestroyApp(&(*it));
		}

		m_listApp.clear();
	}
}

}	// namespace base

