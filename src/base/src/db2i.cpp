#include "db2i.h"

DB2I::DB2I()
{
}

DB2I::~DB2I()
{
}

#define SQLERR_CONNECT_FAIL					(-1000001)
#define SQLERR_DISCONNECT_FAIL				(-1000002)
#define SQLERR_BEGIN_FAIL					(-1000003)
#define SQLERR_COMMIT_FAIL					(-1000004)
#define SQLERR_ROLLBACK_FAIL				(-1000005)
void Connect() throw(SQLException)
{
	throw SQLException(SQLERR_CONNECT_FAIL, "Connect fail!");
}

void Disconnect() throw(SQLException)
{
	throw SQLException(SQLERR_DISCONNECT_FAIL, "Disconnect fail!");
}

void Begin() throw(SQLException)
{
	throw SQLException(SQLERR_BEGIN_FAIL, "Begin fail!");
}

void Commit() throw(SQLException)
{
	throw SQLException(SQLERR_COMMIT_FAIL, "Commit fail!");
}

void Rollback() throw(SQLException)
{
	throw SQLException(SQLERR_ROLLBACK_FAIL, "Rollback fail!");
}

