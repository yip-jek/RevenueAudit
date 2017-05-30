#pragma once

#include <string>

// 告警请求
struct YDAlarmReq
{
public:
	YDAlarmReq();
	~YDAlarmReq() {}

	// 请求状态
	enum REQ_STATUS
	{
		RS_Unknown = -1,		// 状态：未知
		RS_Request = 0,			// 状态：请求
		RS_Finish  = 1,			// 状态：完成
		RS_Error   = 2,			// 状态：处理异常
	};

public:
	// 设置请求状态
	bool SetReqStatus(const std::string& rs);

	// 获取请求状态
	std::string GetReqStatus() const;

public:
	int         seq;					// 流水号
	std::string alarm_date;				// 告警日期
	std::string region;					// 地市
	std::string channel_type;			// 渠道属性
	std::string channel_name;			// 渠道名称
	std::string busi_type;				// 业务分类
	std::string pay_type;				// 支付方式
	REQ_STATUS  status;					// 请求状态
	std::string req_time;				// 请求时间
	std::string finish_time;			// 完成时间
};

// 告警阈值
struct YDAlarmThreshold
{
public:
	YDAlarmThreshold();
	~YDAlarmThreshold() {}

public:
	int         seq;					// 流水号
	std::string region;					// 地市
	std::string channel_type;			// 渠道属性
	std::string channel_name;			// 渠道名称
	std::string responser;				// 告警负责人
	std::string call_no;				// 告警负责人联系方式
	std::string pay_type;				// 支付方式
	double      threshold;				// 阈值
	int         offset;					// 计划完成日期偏移量
	std::string msg_template;			// 短信模板
};

// 告警采集数据
struct YDAlarmSrcData
{
public:

public:
	int         seq;
	std::string 
};

