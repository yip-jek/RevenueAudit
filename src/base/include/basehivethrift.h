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

class BaseHiveThrift
{
public:
	typedef apache::thrift::transport::TTransport			T_THRIFT_TRANSPORT;
	typedef apache::thrift::transport::TSocket				T_THRIFT_SOCKET;
	typedef apache::thrift::transport::TBufferedTransport	T_THRIFT_BUFFER_TRANSPORT;

	typedef apache::thrift::protocol::TProtocol				T_THRIFT_PROTOCOL;
	typedef apache::thrift::protocol::TBinaryProtocol		T_THRIFT_BINARY_PROTOCOL;

	typedef Apache::Hadoop::Hive::ThriftHiveClient			T_THRIFT_HIVE_CLIENT;

public:
	BaseHiveThrift(const std::string& ip, int port);
	virtual ~BaseHiveThrift();

public:
	// 初始化
	virtual void Init();

	// 连接Hive Server
	virtual void Connect() throw(Exception);

	// 断开Hive Server的连接
	virtual void Disconnect();

protected:
	boost::shared_ptr<T_THRIFT_TRANSPORT>	m_spSocket;
	boost::shared_ptr<T_THRIFT_TRANSPORT>	m_spTransport;
	boost::shared_ptr<T_THRIFT_PROTOCOL>	m_spProtocol;
	boost::shared_ptr<T_THRIFT_HIVE_CLIENT>	m_spHiveClient;

protected:
	Log*			m_pLog;
	std::string		m_sIPAddr;
	int				m_nPort;
	bool			m_bConnected;
};

}	// namespace base

