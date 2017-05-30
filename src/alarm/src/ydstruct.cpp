#include "ydstruct.h"
#include "pubstr.h"

YDAlarmReq::YDAlarmReq()
:seq(0)
,status(RS_Unknown)
{
}

bool YDAlarmReq::SetReqStatus(const std::string& rs)
{
	const std::string RS = base::PubStr::TrimB(rs);
	if ( "0" == RS )			// 状态：请求
	{
		status = RS_Request;
	}
	else if ( "1" == RS )		// 状态：完成
	{
		status = RS_Finish;
	}
	else if ( "2" == RS )		// 状态：处理异常
	{
		status = RS_Error;
	}
	else		// 状态：未知
	{
		status = RS_Unknown;
		return false;
	}

	return true;
}

std::string YDAlarmReq::GetReqStatus() const
{
	switch ( status )
	{
	case RS_Request:
		return "0";
	case RS_Finish:
		return "1";
	case RS_Error:
		return "2";
	case RS_Unknown:
	default:
		return "<UNKNOWN>";
	}
}

YDAlarmThreshold::YDAlarmThreshold()
:seq(0)
,threshold(0.0)
,offset(0)
{
}

