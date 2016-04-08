#pragma once

#include "sqlexception.h"

class DB2I
{
public:
	DB2I();
	virtual ~DB2I();

public:
	void Connect() throw(SQLException);
	void Disconnect() throw(SQLException);
	void Begin() throw(SQLException);
	void Commit() throw(SQLException);
	void Rollback() throw(SQLException);

private:
};

