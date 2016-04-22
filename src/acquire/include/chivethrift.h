#pragma once

#include <string>
#include <boost/shared_ptr.hpp>
#include <transport/TTransport.h>
#include <protocol/TProtocol.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include "ThriftHive.h"
#include "exception.h"

namespace base
{
class Log;
}

class CHiveThrift
{
public:
	typedef apache::thrift::transport::TTransport			T_THRIFT_TRANSPORT;
	typedef apache::thrift::transport::TSocket				T_THRIFT_SOCKET;
	typedef apache::thrift::transport::TBufferedTransport	T_THRIFT_BUFFER_TRANSPORT;

	typedef apache::thrift::protocol::TProtocol				T_THRIFT_PROTOCOL;
	typedef apache::thrift::protocol::TBinaryProtocol		T_THRIFT_BINARY_PROTOCOL;

	typedef Apache::Hadoop::Hive::ThriftHiveClient			T_THRIFT_HIVE_CLIENT;

public:
	CHiveThrift(const std::string& ip, int port);
	~CHiveThrift();

public:
	static const int HIVE_MAX_FETCHN = 20000;

	enum HT_ERROR
	{
		HTERR_CONNECT_FAILED    = -3100001,			// 连接Hive Server失败
		HTERR_DISCONNECT_FAILED = -3100002,			// 断开Hive Server的连接失败
		HTERR_APP_EXCEPTION     = -3100003,			// TApplicationException异常
		HTERR_T_EXCEPTION       = -3100004,			// TException异常
	};

public:
	// 初始化
	void Init();

	// 连接Hive Server
	void Connect() throw(base::Exception);

	// 断开Hive Server的连接
	void Disconnect();

	void Test(const std::string& table) throw(base::Exception);

private:
	boost::shared_ptr<T_THRIFT_TRANSPORT>	m_spSocket;
	boost::shared_ptr<T_THRIFT_TRANSPORT>	m_spTransport;
	boost::shared_ptr<T_THRIFT_PROTOCOL>	m_spProtocol;
	boost::shared_ptr<T_THRIFT_HIVE_CLIENT>	m_spHiveClient;

private:
	base::Log*		m_pLog;
	std::string		m_sIPAddr;
	int				m_nPort;
	bool			m_bConnected;
};

