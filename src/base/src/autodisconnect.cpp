#include "autodisconnect.h"
#include "basedb2.h"
#include "basejhive.h"
#include "def.h"

namespace base
{

HiveConnector::HiveConnector(BaseJHive* pHive)
:m_hiveConnected(false)
,m_pHive(pHive)
{
}

void HiveConnector::ToConnect()
{
	if ( NULL == m_pHive )
	{
		throw Exception(AUTODIS_UNABLE_CONNECT, "无法连接HIVE (Pointer is NULL!) [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( !m_hiveConnected )
	{
		m_pHive->Connect();
		m_hiveConnected = true;
	}
}

void HiveConnector::ToDisconnect()
{
	if ( m_hiveConnected && m_pHive != NULL )
	{
		m_pHive->Disconnect();
		m_hiveConnected = false;
	}
}

///////////////////////////////////////////////////////////////////////////////////////
AutoDisconnect::AutoDisconnect(BaseConnector* pConnector)
:m_need2Disconnect(false)
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

void AutoDisconnect::Connect()
{
	if ( NULL == m_pConnector )
	{
		throw Exception(AUTODIS_UNABLE_CONNECT, "连接失败：没有指定连接器！(Pointer is NULL!) [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 先标记
	m_need2Disconnect = true;

	m_pConnector->ToConnect();
}

void AutoDisconnect::Disconnect()
{
	if ( m_need2Disconnect && m_pConnector != NULL )
	{
		// 先标记
		m_need2Disconnect = false;

		m_pConnector->ToDisconnect();
	}
}

}	// namespace base

