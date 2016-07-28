#include "canahive.h"
#include <vector>
#include "log.h"
#include "pubstr.h"

CAnaHive::CAnaHive()
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

