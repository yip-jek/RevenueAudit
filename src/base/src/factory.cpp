#include "factory.h"

namespace base
{

Factory* g_pFactory = NULL;

Factory::Factory()
{
}

Factory::~Factory()
{
}

BaseFrameApp* Factory::Create(const std::string& mode, const std::string& var, std::string* pError)
{
	BaseFrameApp* pApp = CreateApp(mode, var, pError);
	if ( pApp != NULL )
	{
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

