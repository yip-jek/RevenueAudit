#pragma once

#include "exception.h"

namespace base
{

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

// Hive连接器
class HiveConnector : public BaseConnector
{
public:
	explicit HiveConnector(BaseJHive* pHive);
	virtual ~HiveConnector() {}

public:
	virtual void ToConnect() throw(Exception);
	virtual void ToDisconnect();

private:
	bool		m_hiveConnected;
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
	virtual ~AutoDisconnect();

public:
	void Connect() throw(Exception);

private:
	// 只允许内部自动关闭连接
	void Disconnect();

private:
	bool			m_need2Disconnect;
	BaseConnector*	m_pConnector;		// 连接器
};

}	// namespace base

