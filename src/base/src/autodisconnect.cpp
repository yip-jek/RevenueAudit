#include "autodisconnect.h"
#include "basedb2.h"
#include "basejhive.h"
#include "def.h"

namespace base
{

HiveDB2Connector::HiveDB2Connector(BaseDB2* pDB2, BaseJHive* pHive)
:m_pDB2(pDB2)
,m_pHive(pHive)
{
}

void HiveDB2Connector::ToConnect() throw(Exception)
{
	if ( NULL == m_pHive )
	{
		throw Exception(AUTODIS_UNABLE_CONNECT, "无法连接HIVE (Pointer is NULL!) [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}
	m_pHive->Connect();

	if ( NULL == m_pDB2 )
	{
		throw Exception(AUTODIS_UNABLE_CONNECT, "无法连接DB2 (Pointer is NULL!) [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}
	m_pDB2->Connect();
}

void HiveDB2Connector::ToDisconnect()
{
	if ( m_pDB2 != NULL )
	{
		m_pDB2->Disconnect();
	}

	if ( m_pHive != NULL )
	{
		m_pHive->Disconnect();
	}
}

///////////////////////////////////////////////////////////////////////////////////////
AutoDisconnect::AutoDisconnect(BaseConnector* pConnector)
:m_bConnected(false)
,m_pConnector(pConnector)
{
}

AutoDisconnect::~AutoDisconnect()
{
	Disconnect();

	if ( m_pConnector != NULL )
	{
		delete m_pConnector;
		m_pConnector = NULL;
	}
}

bool AutoDisconnect::IsConnected() const
{
	return m_bConnected;
}

void AutoDisconnect::Connect() throw(Exception)
{
	if ( !m_bConnected )
	{
		if ( NULL == m_pConnector )
		{
			throw Exception(AUTODIS_UNABLE_CONNECT, "连接失败：没有指定连接器！(Pointer is NULL!) [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		}

		m_pConnector->ToConnect();

		m_bConnected = true;
	}
}

void AutoDisconnect::Disconnect()
{
	if ( m_bConnected )
	{
		if ( m_pConnector != NULL )
		{
			m_pConnector->ToDisconnect();
		}

		m_bConnected = false;
	}
}

}	// namespace base

