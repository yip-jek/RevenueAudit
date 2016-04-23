#pragma once

#include "basedb2.h"

class CAcqDB2 : public base::BaseDB2
{
public:
	CAcqDB2(const std::string& db_name, const std::string& usr, const std::string& pw);
	virtual ~CAcqDB2();

private:
};

