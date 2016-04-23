#include "basehivethrift.h"
#include "def.h"
#include "log.h"

namespace base
{

BaseHiveThrift::BaseHiveThrift(const std::string& ip, int port)
:m_pLog(Log::Instance())
,m_sIPAddr(ip)
,m_nPort(port)
,m_bConnected(false)
{
}

BaseHiveThrift::~BaseHiveThrift()
{
	Disconnect();
}

void BaseHiveThrift::Init()
{
	m_spSocket.reset(new T_THRIFT_SOCKET(m_sIPAddr, m_nPort));

	m_spTransport.reset(new T_THRIFT_BUFFER_TRANSPORT(m_spSocket));

	m_spProtocol.reset(new T_THRIFT_BINARY_PROTOCOL(m_spTransport));

	m_spHiveClient.reset(new T_THRIFT_HIVE_CLIENT(m_spProtocol));
}

void BaseHiveThrift::Connect() throw(Exception)
{
	if ( !m_bConnected )
	{
		try
		{
			m_spTransport->open();
		}
		catch ( const apache::thrift::transport::TTransportException& ex )
		{
			throw Exception(BHT_CONNECT_FAILED, "[HIVE] Connect <Host:%s, Port:%d> failed! [TTransportException] %s [FILE:%s, LINE:%d]", m_sIPAddr.c_str(), m_nPort, ex.what(), __FILE__, __LINE__);
		}

		m_bConnected = true;

		m_pLog->Output("[HIVE] Connect <Host:%s, Port:%d> OK.", m_sIPAddr.c_str(), m_nPort);
	}
}

void BaseHiveThrift::Disconnect()
{
	if ( m_bConnected )
	{
		m_spTransport->close();

		m_bConnected = false;

		m_pLog->Output("[HIVE] Disconnected.");
	}
}

}

