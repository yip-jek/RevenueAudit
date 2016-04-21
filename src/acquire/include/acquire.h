#pragma once

#include <boost/shared_ptr.hpp>
#include <transport/TTransport.h>
#include <protocol/TProtocol.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include "ThriftHive.h"
#include "baseframeapp.h"


// 采集模块
class Acquire : public base::BaseFrameApp
{
public:
	typedef apache::thrift::transport::TTransport			T_THRIFT_TRANSPORT;
	typedef apache::thrift::transport::TSocket				T_THRIFT_SOCKET;
	typedef apache::thrift::transport::TBufferedTransport	T_THRIFT_BUFFER_TRANSPORT;

	typedef apache::thrift::protocol::TProtocol				T_THRIFT_PROTOCOL;
	typedef apache::thrift::protocol::TBinaryProtocol		T_THRIFT_BINARY_PROTOCOL;

	typedef Apache::Hadoop::Hive::ThriftHiveClient			T_THRIFT_HIVE_CLIENT;

public:
	Acquire();
	virtual ~Acquire();

public:
	static const int HIVE_MAX_FETCHN = 20000;

	enum ACQ_ERROR
	{
		ACQERR_TASKINFO_ERROR    = -3000001,			// 任务信息异常
		ACQERR_KPIID_INVALID     = -3000002,			// 指标ID无效
		ACQERR_ETLID_INVALID     = -3000003,			// 采集规则ID无效
		ACQERR_HIVE_PORT_INVALID = -3000004,			// Hive服务器端口无效
		ACQERR_APP_EXCEPTION     = -3000005,			// TApplicationException异常
		ACQERR_T_EXCEPTION       = -3000006,			// TException异常
	};

public:
	// 版本信息
	virtual const char* Version();

	// 载入参数配置信息
	virtual void LoadConfig() throw(base::Exception);

	// 初始化
	virtual void Init() throw(base::Exception);

	// 任务执行
	virtual void Run() throw(base::Exception);

private:
	// 释放资源
	void Release();

	// 获取参数任务信息
	void GetParameterTaskInfo() throw(base::Exception);

private:
	long		m_nKpiID;		// 指标ID
	long		m_nEtlID;		// 采集规则ID

	std::string m_sDBName;		// 数据库名称
	std::string m_sUsrName;		// 用户名
	std::string m_sPasswd;		// 密码

	std::string m_sHiveIP;		// Hive服务器IP地址
	int			m_nHivePort;	// Hive服务器端口

private:
	boost::shared_ptr<T_THRIFT_TRANSPORT>	m_spSocket;
	boost::shared_ptr<T_THRIFT_TRANSPORT>	m_spTransport;
	boost::shared_ptr<T_THRIFT_PROTOCOL>	m_spProtocol;
	T_THRIFT_HIVE_CLIENT*					m_pTHiveClient;
};

