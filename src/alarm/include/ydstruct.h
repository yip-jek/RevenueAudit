#pragma once

#include <string>

// 告警请求
struct YDAlarmReq
{
public:
	YDAlarmReq();
	~YDAlarmReq() {}

	static const char* const S_MARK_ANY;		// 任意值标志：*

	// 请求状态
	enum REQ_STATUS
	{
		RS_Unknown = -1,		// 状态：未知
		RS_Request = 0,			// 状态：请求
		RS_Finish  = 1,			// 状态：完成
		RS_Error   = 2,			// 状态：处理异常
	};

public:
	// 日志打印信息
	std::string LogPrintInfo();

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
	std::string request_time;			// 请求时间
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
	int         offset;					// 计划完成日期偏移量（单位：天）
	std::string msg_template;			// 短信模板
};

// 告警数据
struct YDAlarmData
{
public:
	YDAlarmData();
	~YDAlarmData() {}

public:
	// 日志打印信息
	std::string LogPrintInfo();

public:
	int         seq;					// 流水号
	std::string alarm_date;				// 告警日期
	std::string manage_level;			// 管理级别（地市）
	std::string channel_attr;			// 渠道属性
	std::string channel_name;			// 渠道名称
	std::string bus_sort;				// 业务分类
	std::string pay_code;				// 支付方式
	double      arrears;				// 欠费
};

// 告警信息
struct YDAlarmInfo
{
public:
	YDAlarmInfo();
	~YDAlarmInfo() {}

	static const char* const S_ALARM_DATE;			// 标记：告警日期
	static const char* const S_REGION;				// 标记：地市
	static const char* const S_CHANN_TYPE;			// 标记：渠道属性
	static const char* const S_CHANN_NAME;			// 标记：渠道名称
	static const char* const S_BUSI_TYPE;			// 标记：业务分类
	static const char* const S_PAY_TYPE;			// 标记：支付方式
	static const char* const S_RESPONSER;			// 标记：告警负责人
	static const char* const S_GENE_TIME;			// 标记：告警生成时间
	static const char* const S_PLAN_TIME;			// 标记：告警计划完成时间
	static const char* const S_ARREARS;				// 标记：欠费

public:
	// 日志打印信息
	std::string LogPrintInfo();

	// 通过特定标记获得信息内容
	std::string GetInfoByMark(const std::string& mark) const;

public:
	int         seq;					// 流水号
	std::string alarm_date;				// 告警日期
	std::string region;					// 地市
	std::string channel_type;			// 渠道属性
	std::string channel_name;			// 渠道名称
	std::string busi_type;				// 业务分类
	std::string pay_type;				// 支付方式
	std::string responser;				// 告警负责人
	std::string call_no;				// 告警负责人联系方式
	std::string generate_time;			// 告警生成时间
	std::string plan_time;				// 告警计划完成时间
	std::string msg_template;			// 短信模板
	double      arrears;				// 欠费
};

