#include "chivethrift.h"
#include <vector>
#include "log.h"

CHiveThrift::CHiveThrift(const std::string& ip, int port)
:m_pLog(base::Log::Instance())
,m_sIPAddr(ip)
,m_nPort(port)
,m_bConnected(false)
{
}

CHiveThrift::~CHiveThrift()
{
	Disconnect();
}

void CHiveThrift::Init()
{
	m_spSocket.reset(new T_THRIFT_SOCKET(m_sIPAddr, m_nPort));

	m_spTransport.reset(new T_THRIFT_BUFFER_TRANSPORT(m_spSocket));

	m_spProtocol.reset(new T_THRIFT_BINARY_PROTOCOL(m_spTransport));

	m_spHiveClient.reset(new T_THRIFT_HIVE_CLIENT(m_spProtocol));
}

void CHiveThrift::Connect() throw(base::Exception)
{
	if ( !m_bConnected )
	{
		try
		{
			m_spTransport->open();
		}
		catch ( const apache::thrift::transport::TTransportException& ex )
		{
			throw base::Exception(HTERR_CONNECT_FAILED, "Connect <Host:%s, Port:%d> failed! [TTransportException] %s [FILE:%s, LINE:%d]", m_sIPAddr.c_str(), m_nPort, ex.what(), __FILE__, __LINE__);
		}

		m_bConnected = true;

		m_pLog->Output("[HIVE] Connect <Host:%s, Port:%d> OK.", m_sIPAddr.c_str(), m_nPort);
	}
}

void CHiveThrift::Disconnect()
{
	if ( m_bConnected )
	{
		m_spTransport->close();

		m_bConnected = false;

		m_pLog->Output("[HIVE] Disconnected.");
	}
}

void CHiveThrift::Test(const std::string& table) throw(base::Exception)
{
	try
	{
		std::string test_sql = "select * from " + table;
		m_pLog->Output("[HIVE] Query sql: %s", test_sql.c_str());

		m_pLog->Output("[HIVE] Execute query sql ...");
		m_spHiveClient->execute(test_sql);
		m_pLog->Output("[HIVE] Execute query sql OK.");

		std::vector<std::string> vec_str;
		long total = 0;
		do
		{
			vec_str.clear();

			m_spHiveClient->fetchN(vec_str, HIVE_MAX_FETCHN);

			const int V_SIZE = vec_str.size();
			for ( int i = 0; i < V_SIZE; ++i )
			{
				m_pLog->Output("[GET] %d> %s", ++total, vec_str[i].c_str());
			}
		} while ( vec_str.size() > 0 );
		m_pLog->Output("[HIVE] Get %ld row(s)", total);
	}
	catch ( const apache::thrift::TApplicationException& ex )
	{
		throw base::Exception(HTERR_APP_EXCEPTION, "[TApplicationException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
	catch ( const apache::thrift::TException& ex )
	{
		throw base::Exception(HTERR_T_EXCEPTION, "[TException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
	}
}

