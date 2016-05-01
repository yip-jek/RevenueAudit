#include "autodisconnect.h"
#include "basedb2.h"
#include "basehivethrift.h"

namespace base
{

AutoDisconnect::AutoDisconnect(BaseDB2* pDB2, BaseHiveThrift* pHive)
:m_bConnected(false)
,m_pDB2(pDB2)
,m_pHive(pHive)
{
}

AutoDisconnect::~AutoDisconnect()
{
	Disconnect();
}

bool AutoDisconnect::IsConnected() const
{
	return m_bConnected;
}

void AutoDisconnect::Connect() throw(Exception)
{
	if ( !m_bConnected )
	{
		if ( m_pDB2 != NULL )
		{
			m_pDB2->Connect();
		}

		if ( m_pHive != NULL )
		{
			m_pHive->Connect();
		}

		m_bConnected = true;
	}
}

void AutoDisconnect::Disconnect()
{
	if ( m_bConnected )
	{
		if ( m_pDB2 != NULL )
		{
			m_pDB2->Disconnect();
		}

		if ( m_pHive != NULL )
		{
			m_pHive->Disconnect();
		}

		m_bConnected = false;
	}
}

}	// namespace base

