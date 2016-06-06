#include "hdfsconnector.h"
#include "log.h"

HdfsConnector::HdfsConnector(const std::string& host, int port)
:m_pLogger(base::Log::Instance())
,m_sHost(host)
,m_nPort(port)
,m_hdfsFS(NULL)
{
}

HdfsConnector::~HdfsConnector()
{
	base::Log::Release();
}

void HdfsConnector::ToConnect() throw(base::Exception)
{
	if ( m_hdfsFS != NULL )
	{
		throw base::Exception(HCERR_CONNECT_FAILED, "[HDFS] 重复连接：已经连接！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_hdfsFS = hdfsConnect(m_sHost.c_str(), m_nPort);
	if ( NULL == m_hdfsFS )
	{
		throw base::Exception(HCERR_CONNECT_FAILED, "[HDFS] Connect failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLogger->Output("[HDFS] Connect <%s:%d> OK.", m_sHost.c_str(), m_nPort);
}

void HdfsConnector::ToDisconnect()
{
	if ( m_hdfsFS != NULL )
	{
		hdfsDisconnect(m_hdfsFS);
		m_hdfsFS = NULL;

		m_pLogger->Output("[HDFS] Disconnected.");
	}
}

hdfsFS HdfsConnector::GetHdfsFS() const
{
	return m_hdfsFS;
}

