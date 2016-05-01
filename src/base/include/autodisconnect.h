#pragma once

#include "exception.h"

namespace base
{

class BaseDB2;
class BaseHiveThrift;

class AutoDisconnect
{
private:	// noncopyable
	AutoDisconnect(const AutoDisconnect& );
	const AutoDisconnect& operator = (const AutoDisconnect& );

public:
	AutoDisconnect(BaseDB2* pDB2, BaseHiveThrift* pHive);
	~AutoDisconnect();

public:
	bool IsConnected() const;
	void Connect() throw(Exception);
	void Disconnect();

private:
	bool			m_bConnected;		// 连接的标记
	BaseDB2*		m_pDB2;				// DB2数据库接口
	BaseHiveThrift*	m_pHive;			// Hive接口
};

}	// namespace base

