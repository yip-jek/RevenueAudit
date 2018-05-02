#pragma once

#include <string>
#include "xdbo2/database.h"
#include "exception.h"

namespace base
{

class Log;

class BaseDB2
{
public:
	BaseDB2(const std::string& db_name, const std::string& usr, const std::string& pw);
	virtual ~BaseDB2();

public:
	// 连接数据库
	virtual void Connect() throw(Exception);

	// 断开数据库连接
	virtual void Disconnect() throw(Exception);

	// 开始事务
	virtual void Begin() throw(Exception);

	// 提交事务
	virtual void Commit(bool output = true) throw(Exception);

	// 回滚事务
	virtual void Rollback() throw(Exception);

protected:
	Log*			m_pLog;
	std::string		m_sDBName;
	std::string		m_sUsrName;
	std::string		m_sPasswd;
	bool			m_bConnected;

protected:
	XDBO2::CDatabase	m_CDB;
};

}	// namespace base

