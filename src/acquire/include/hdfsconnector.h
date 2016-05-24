#pragma once

#include "autodisconnect.h"
#include "hdfs.h"

class HdfsConnector : public base::BaseConnector
{
public:
	HdfsConnector(const std::string& host, int port);
	virtual ~HdfsConnector();

public:
	enum HDFS_CONN_ERROR
	{
		HCERR_CONNECT_FAILED	= -2004001,		// 连接失败
	};

public:
	virtual void ToConnect() throw(base::Exception);
	virtual void ToDisconnect();

public:
	hdfsFS GetHdfsFS() const;

private:
	std::string		m_sHost;		// 主机信息
	int				m_nPort;		// 端口号

	hdfsFS			m_hdfsFS;
};

