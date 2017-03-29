#include "canahive.h"
#include <vector>
#include "log.h"
#include "pubstr.h"
#include "anaerror.h"

CAnaHive::CAnaHive(const std::string& hive_jclassname)
:base::BaseJHive(hive_jclassname)
{
}

CAnaHive::~CAnaHive()
{
}

void CAnaHive::FetchSourceData(const std::string& hive_sql, std::vector<std::vector<std::string> >& vec2_fields) throw(base::Exception)
{
	if ( hive_sql.empty() )
	{
		throw base::Exception(HTERR_FETCH_SRCDATA_FAILED, "[HIVE] Fetch source data failed: no sql to be executed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	try
	{
		m_pLog->Output("[HIVE] Fetch source data ...");
		FetchSQL(hive_sql, vec2_fields);
		m_pLog->Output("[HIVE] Fetch source data ---- done!");
	}
	catch ( const base::Exception& ex )
	{
		throw base::Exception(HTERR_FETCH_SRCDATA_FAILED, "[HIVE] Fetch source data failed! %s [FILE:%s, LINE:%d]", ex.What().c_str(), __FILE__, __LINE__);
	}
}

void CAnaHive::ExecuteAnaSQL(const std::string& hive_sql) throw(base::Exception)
{
	if ( hive_sql.empty() )
	{
		throw base::Exception(HTERR_EXECUTE_SQL_FAILED, "[HIVE] Execute SQL failed: no sql to be executed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	try
	{
		ExecuteSQL(hive_sql);
	}
	catch ( const base::Exception& ex )
	{
		throw base::Exception(HTERR_EXECUTE_SQL_FAILED, "[HIVE] Execute SQL failed! %s [FILE:%s, LINE:%d]", ex.What().c_str(), __FILE__, __LINE__);
	}
}

