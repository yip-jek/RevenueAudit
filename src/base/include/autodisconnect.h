#pragma once

#include "exception.h"

namespace base
{

class BaseDB2;
class BaseJHive;

// 连接器接口
class BaseConnector
{
public:
	BaseConnector() {}
	virtual ~BaseConnector() {}

public:
	virtual void ToConnect() throw(Exception) = 0;
	virtual void ToDisconnect() = 0;
};

// Hive和DB2连接器
class HiveDB2Connector : public BaseConnector
{
public:
	HiveDB2Connector(BaseDB2* pDB2, BaseJHive* pHive);
	virtual ~HiveDB2Connector() {}

public:
	virtual void ToConnect() throw(Exception);
	virtual void ToDisconnect();

private:
	BaseDB2*	m_pDB2;				// DB2数据库接口
	BaseJHive*	m_pHive;			// Hive接口
};

// 自动关闭连接
class AutoDisconnect
{
private:	// noncopyable
	AutoDisconnect(const AutoDisconnect& );
	const AutoDisconnect& operator = (const AutoDisconnect& );

public:
	// 连接器资源自动释放
	explicit AutoDisconnect(BaseConnector* pConnector);
	~AutoDisconnect();

public:
	bool IsConnected() const;
	void Connect() throw(Exception);
	void Disconnect();

private:
	bool			m_bConnected;		// 连接的标记
	BaseConnector*	m_pConnector;		// 连接器
};

}	// namespace base

