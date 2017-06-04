#include "ydstruct.h"
#include "pubstr.h"

const char* const YDAlarmReq::S_MARK_ANY = "*";			// 任意值标志：*

YDAlarmReq::YDAlarmReq()
:seq(0)
,status(RS_Unknown)
{
}

std::string YDAlarmReq::LogPrintInfo()
{
	std::string log_info;
	base::PubStr::SetFormatString(log_info, "SEQ=[%d], ALARM_DATE=[%s], REGION=[%s], CHANN_TYPE=[%s], CHANN_NAME=[%s], BUS_TYPE=[%s], PAY_TYPE=[%s], REQUEST_TIME=[%s]", seq, alarm_date.c_str(), region.c_str(), channel_type.c_str(), channel_name.c_str(), busi_type.c_str(), pay_type.c_str(), request_time.c_str());
	return log_info;
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

YDAlarmData::YDAlarmData()
:seq(0)
,arrears(0.0)
{
}

std::string YDAlarmData::LogPrintInfo()
{
	std::string log_info;
	base::PubStr::SetFormatString(log_info, "SEQ=[%d], ALARM_DATE=[%s], MANAGE_LV=[%s], CHANN_ATTR=[%s], CHANN_NAME=[%s], BUS_SORT=[%s], PAY_CODE=[%s], ARREARS=[%s]", seq, alarm_date.c_str(), manage_level.c_str(), channel_attr.c_str(), channel_name.c_str(), bus_sort.c_str(), pay_code.c_str(), base::PubStr::Double2FormatStr(arrears).c_str());
	return log_info;
}


const char* const YDAlarmInfo::S_ALARM_DATE = "ALARM_DATE";			// 标记：告警日期
const char* const YDAlarmInfo::S_REGION     = "REGION";				// 标记：地市
const char* const YDAlarmInfo::S_CHANN_TYPE = "CHANN_TYPE";			// 标记：渠道属性
const char* const YDAlarmInfo::S_CHANN_NAME = "CHANN_NAME";			// 标记：渠道名称
const char* const YDAlarmInfo::S_BUSI_TYPE  = "BUSI_TYPE";			// 标记：业务分类
const char* const YDAlarmInfo::S_PAY_TYPE   = "PAY_TYPE";			// 标记：支付方式
const char* const YDAlarmInfo::S_RESPONSER  = "RESPONSER";			// 标记：告警负责人
const char* const YDAlarmInfo::S_GENE_TIME  = "GENE_TIME";			// 标记：告警生成时间
const char* const YDAlarmInfo::S_PLAN_TIME  = "PLAN_TIME";			// 标记：告警计划完成时间
const char* const YDAlarmInfo::S_ARREARS    = "ARREARS";			// 标记：欠费

YDAlarmInfo::YDAlarmInfo()
:seq(0), arrears(0.0)
{
}

std::string YDAlarmInfo::LogPrintInfo()
{
	std::string log_info;
	base::PubStr::SetFormatString(log_info, "SEQ=[%d], ALARM_DATE=[%s], REGION=[%s], CHANN_TYPE=[%s], CHANN_NAME=[%s], BUS_TYPE=[%s], PAY_TYPE=[%s], RESPONSER=[%s], CALL_NO=[%s], GENE_TIME=[%s], PLAN_TIME=[%s]", seq, alarm_date.c_str(), region.c_str(), channel_type.c_str(), channel_name.c_str(), busi_type.c_str(), pay_type.c_str(), responser.c_str(), call_no.c_str(), generate_time.c_str(), plan_time.c_str());
	return log_info;
}

std::string YDAlarmInfo::GetInfoByMark(const std::string& mark) const
{
	const std::string MARK = base::PubStr::TrimUpperB(mark);

	if ( S_ALARM_DATE == MARK )// 标记：告警日期
	{
		return alarm_date;
	}
	else if ( S_REGION == MARK )// 标记：地市
	{
		return region;
	}
	else if ( S_CHANN_TYPE == MARK )// 标记：渠道属性
	{
		return channel_type;
	}
	else if ( S_CHANN_NAME == MARK )// 标记：渠道名称
	{
		return channel_name;
	}
	else if ( S_BUSI_TYPE == MARK )// 标记：业务分类
	{
		return busi_type;
	}
	else if ( S_PAY_TYPE == MARK )// 标记：支付方式
	{
		return pay_type;
	}
	else if ( S_RESPONSER == MARK )// 标记：告警负责人
	{
		return responser;
	}
	else if ( S_GENE_TIME == MARK )// 标记：告警生成时间
	{
		return generate_time;
	}
	else if ( S_PLAN_TIME == MARK )// 标记：告警计划完成时间
	{
		return plan_time;
	}
	else if ( S_ARREARS == MARK )// 标记：欠费
	{
		return base::PubStr::Double2FormatStr(arrears);
	}
	else
	{
		return "";
	}
}

