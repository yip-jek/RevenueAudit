#include "acqtaskinfo.h"

OneEtlDim::OneEtlDim()
:EtlDimSeq(0)
,EtlDimMemo(DMTYPE_NONE)
{
}

void OneEtlDim::SetDimMemoType(const std::string& m_type)
{
	const std::string TYPE = base::PubStr::TrimUpperB(m_type);

	if ( "JOIN_ON" == TYPE )		// 外连-关联类型
	{
		EtlDimMemo = DMTYPE_JOIN_ON;
	}
	else if ( "JOIN_DIM" == TYPE )	// 外连-维度类型
	{
		EtlDimMemo = DMTYPE_JOIN_DIM;
	}
	else if ( "SHOW_UP" == TYPE )	// 显示类型
	{
		EtlDimMemo = DMTYPE_SHOW_UP;
	}
	else	// 无
	{
		EtlDimMemo = DMTYPE_NONE;
	}
}

std::string OneEtlDim::GetDimMemoTypeStr() const
{
	switch ( EtlDimMemo )
	{
	case DMTYPE_NONE:
		return std::string("<NONE>");
	case DMTYPE_JOIN_ON:
		return std::string("JOIN_ON");
	case DMTYPE_JOIN_DIM:
		return std::string("JOIN_DIM");
	case DMTYPE_SHOW_UP:
		return std::string("SHOW_UP");
	default:
		return std::string("<-Unknown->");
	}
}

////////////////////////////////////////////////////////////////////////////////
OneEtlVal::OneEtlVal()
:EtlValSeq(0)
,EtlValMemo(VMTYPE_NONE)
{
}

void OneEtlVal::SetValMemoType(const std::string& m_type)
{
	const std::string TYPE = base::PubStr::TrimUpperB(m_type);

	if ( "JOIN_VAL" == TYPE )		// 外连-值类型
	{
		EtlValMemo = VMTYPE_JOIN_VAL;
	}
	//else if ( "SHOW_UP" == TYPE )	// 显示类型
	//{
	//	EtlValMemo = VMTYPE_SHOW_UP;
	//}
	else	// 无
	{
		EtlValMemo = VMTYPE_NONE;
	}
}

std::string OneEtlVal::GetValMemoTypeStr() const
{
	switch ( EtlValMemo )
	{
	case VMTYPE_NONE:
		return std::string("<NONE>");
	case VMTYPE_JOIN_VAL:
		return std::string("JOIN_VAL");
	//case VMTYPE_SHOW_UP:
	//	return std::string("SHOW_UP");
	default:
		return std::string("<-Unknown->");
	}
}

////////////////////////////////////////////////////////////////////////////////
EtlSrcInfo::EtlSrcInfo()
:src_type(ESTYPE_UNKNOWN)
{
}

bool EtlSrcInfo::SetEtlSrcType(const std::string& type)
{
	const std::string C_TYPE = base::PubStr::TrimUpperB(type);

	if ( "STRAIGHT" == C_TYPE )
	{
		src_type = ESTYPE_STRAIGHT;
	}
	else
	{
		src_type = ESTYPE_UNKNOWN;
		return false;
	}

	return true;
}

////////////////////////////////////////////////////////////////////////////////
AcqTaskInfo::AcqTaskInfo()
:EtlCondType(ETLCTYPE_UNKNOWN)
,DataSrcType(DSTYPE_UNKNOWN)
{
}

bool AcqTaskInfo::SetConditionType(const std::string& type)
{
	std::string c_type = base::PubStr::TrimUpperB(type);

	if ( "NONE" == c_type )		// 不带条件
	{
		EtlCondType = ETLCTYPE_NONE;
	}
	else if ( "STRAIGHT" == c_type )	// 直接条件
	{
		EtlCondType = ETLCTYPE_STRAIGHT;
	}
	else if ( "OUTER_JOIN" == c_type )	// 外连条件
	{
		EtlCondType = ETLCTYPE_OUTER_JOIN;
	}
	else if ( "OUTER_JOIN_WITH_COND" == c_type )
	{
		EtlCondType = ETLCTYPE_OUTER_JOIN_WITH_COND;
	}
	else	// ERROR: 未知条件
	{
		EtlCondType = ETLCTYPE_UNKNOWN;
		return false;
	}

	return true;
}

bool AcqTaskInfo::SetDataSourceType(const std::string& type)
{
	std::string ds_type = base::PubStr::TrimUpperB(type);

	if ( "HIVE" == ds_type )		// HIVE类型
	{
		DataSrcType = DSTYPE_HIVE;
	}
	else if ( "DB2" == ds_type )		// DB2类型
	{
		DataSrcType = DSTYPE_DB2;
	}
	else	// ERROR: 未知类型
	{
		DataSrcType = DSTYPE_UNKNOWN;
		return false;
	}

	return true;
}

