#include "basedb2.h"
#include <iostream>
#include "log.h"

namespace base
{

BaseDB2::BaseDB2(const std::string& db_name, const std::string& usr, const std::string& pw)
:m_pLog(Log::Instance())
,m_sDBName(db_name)
,m_sUsrName(usr)
,m_sPasswd(pw)
,m_bConnected(false)
{
	XDBO2::CDatabaseEnv::SetDefaultDriver("db2");
}

BaseDB2::~BaseDB2()
{
	Disconnect();

	Log::Release();
}

void BaseDB2::Connect() throw(Exception)
{
	if ( !m_bConnected )
	{
		try
		{
			if ( !m_CDB.Open(m_sDBName, m_sUsrName, m_sPasswd) )
			{
				throw Exception(BDB_CONNECT_FAILED, "[DB2] Connect to <DB:%s, user:%s, passwd:%s> failed! [FILE:%s, LINE:%d]", m_sDBName.c_str(), m_sUsrName.c_str(), m_sPasswd.c_str(), __FILE__, __LINE__);
			}
		}
		catch ( const XDBO2::CDBException& ex )
		{
			throw Exception(BDB_CONNECT_FAILED, "[DB2] Connect to <DB:%s, user:%s, passwd:%s> failed! [CDBException] %s [FILE:%s, LINE:%d]", m_sDBName.c_str(), m_sUsrName.c_str(), m_sPasswd.c_str(), ex.what(), __FILE__, __LINE__);
		}

		m_bConnected = true;

		std::cout << "[DB2] Connect <DB:" << m_sDBName  << "> OK." << std::endl;
		m_pLog->Output("[DB2] Connect <DB:%s> OK.", m_sDBName.c_str());
	}
}

void BaseDB2::Disconnect() throw(Exception)
{
	if ( m_bConnected )
	{
		try
		{
			if ( !m_CDB.Close() )
			{
				throw Exception(BDB_DISCONNECT_FAILED, "[DB2] Disconnect <DB:%s> failed! [FILE:%s, LINE:%d]", m_sDBName.c_str(), __FILE__, __LINE__);
			}
		}
		catch ( const XDBO2::CDBException& ex )
		{
			throw Exception(BDB_DISCONNECT_FAILED, "[DB2] Disconnect <DB:%s> failed! [CDBException] %s [FILE:%s, LINE:%d]", m_sDBName.c_str(), ex.what(), __FILE__, __LINE__);
		}

		m_bConnected = false;

		std::cout << "[DB2] Disconnect <DB:" << m_sDBName << "> OK." << std::endl;
		m_pLog->Output("[DB2] Disconnect <DB:%s> OK.", m_sDBName.c_str());
	}
}

void BaseDB2::Begin() throw(Exception)
{
	if ( m_bConnected )
	{
		try
		{
			if ( !m_CDB.BeginTrans() )
			{
				throw Exception(BDB_BEGIN_FAILED, "[DB2] Begin transaction failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
			}
		}
		catch ( const XDBO2::CDBException& ex )
		{
			throw Exception(BDB_BEGIN_FAILED, "[DB2] Begin transaction failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
		}

		std::cout << "[DB2] Begin transaction OK." << std::endl;
		m_pLog->Output("[DB2] Begin transaction OK.");
	}
}

void BaseDB2::Commit() throw(Exception)
{
	if ( m_bConnected )
	{
		try
		{
			if ( !m_CDB.CommitTrans(false) )
			{
				throw Exception(BDB_COMMIT_FAILED, "[DB2] Commit failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
			}
		}
		catch ( const XDBO2::CDBException& ex )
		{
			throw Exception(BDB_COMMIT_FAILED, "[DB2] Commit failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
		}

		std::cout << "[DB2] Commit OK." << std::endl;
		m_pLog->Output("[DB2] Commit OK.");
	}
}

void BaseDB2::Rollback() throw(Exception)
{
	if ( m_bConnected )
	{
		try
		{
			if ( !m_CDB.Rollback() )
			{
				throw Exception(BDB_ROLLBACK_FAILED, "[DB2] Rollback failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
			}
		}
		catch ( const XDBO2::CDBException& ex )
		{
			throw Exception(BDB_ROLLBACK_FAILED, "[DB2] Rollback failed! [CDBException] %s [FILE:%s, LINE:%d]", ex.what(), __FILE__, __LINE__);
		}

		std::cout << "[DB2] Rollback OK." << std::endl;
		m_pLog->Output("[DB2] Rollback OK.");
	}
}

}

