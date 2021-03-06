#pragma once

#include <string>
#include <vector>
#include "pubstr.h"

// 任务请求信息
struct YCTaskReq
{
public:
	YCTaskReq(): seq(0), task_batch(0)
	{}

	std::string LogPrintInfo() const
	{
		std::string info;
		base::PubStr::SetFormatString(info, "SEQ=[%d], "
											"STATE=[%s], "
											"STATE_DESC=[%s], "
											"STAT_CYCLE=[%s], "
											"TASK_CITY=[%s], "
											"TASK_BATCH=[%d], "
											"TASK_DESC=[%s], "
											"ACTOR=[%s], "
											"OPER=[%s], "
											"REQ_TYPE=[%s]", 
											seq, 
											state.c_str(), 
											state_desc.c_str(), 
											stat_cycle.c_str(), 
											task_city.c_str(), 
											task_batch, 
											task_desc.c_str(), 
											actor.c_str(), 
											oper.c_str(), 
											req_type.c_str());
		return info;
	}

public:
	int         seq;				// 任务流水号
	std::string state;				// 任务状态
	std::string state_desc;			// 任务状态说明
	std::string stat_cycle;			// 任务账期
	std::string task_city;			// 任务地市
	int         task_batch;			// 任务批次
	std::string task_desc;			// 备注
	std::string actor;				// 角色
	std::string oper;				// 操作员
	std::string req_type;			// 请求类型
};

// 地市核对表批次信息
struct YCHDBBatch
{
public:
	YCHDBBatch(): stat_batch(0) {}

public:
	std::string stat_report;			// 关联报表
	std::string stat_id;				// 统计指标ID
	std::string stat_date;				// 统计账期
	std::string stat_city;				// 统计地市
	int         stat_batch;				// 统计批次
};

// (业财稽核) 分类因子
struct YCCategoryFactor
{
public:
	std::string dim_id;					// 维度ID
	std::string item;					// 数据项名称
	std::string value;					// 维度值
};

// (业财稽核) 分类因子对
struct YCPairCategoryFactor
{
public:
	YCPairCategoryFactor(): index(0)
	{}

public:
	int              index;				// 序号
	YCCategoryFactor cf_A;				// （左A）分类因子
	YCCategoryFactor cf_B;				// （右B）分类因子
};

// (业财稽核) 因子规则信息
struct YCStatInfo
{
public:
	YCStatInfo(): property(0)
	{}

	// 因子规则信息属性
	enum SI_Property
	{
		SI_PROPERTY_VIRTUAL  = 0x0001,				// 虚因子：只参与计算，不入库
		SI_PROPERTY_CATEGORY = 0x0002,				// 分类因子
	};

public:
	// 增加属性
	void AddProperty(SI_Property prop)
	{
		property |= prop;
	}

	// 去除属性
	void RemoveProperty(SI_Property prop)
	{
		property &= (~prop);
	}

	// 检查属性
	bool CheckProperty(SI_Property prop) const
	{
		return (property & prop);
	}

	// 是否为虚因子？
	bool IsVirtual() const
	{
		return CheckProperty(SI_PROPERTY_VIRTUAL);
	}

	// 是否为分类因子？
	bool IsCategory() const
	{
		return CheckProperty(SI_PROPERTY_CATEGORY);
	}

	// 设置维度
	void SetDim(const std::string& dim)
	{
		// 虚因子标记
		const std::string VIRTUAL = "[VIRTUAL]";
		const size_t VIRTUAL_SIZE = VIRTUAL.size();

		// 是否为虚因子？
		const std::string TP_DIM = base::PubStr::TrimUpperB(dim);
		if ( VIRTUAL == TP_DIM.substr(0, VIRTUAL_SIZE) )
		{
			AddProperty(SI_PROPERTY_VIRTUAL);
			statdim_id = base::PubStr::TrimB(TP_DIM.substr(VIRTUAL_SIZE));
		}
		else
		{
			RemoveProperty(SI_PROPERTY_VIRTUAL);
			statdim_id = TP_DIM;
		}

		// 是否为分类因子？
		if ( statdim_id.find('?') != std::string::npos )
		{
			AddProperty(SI_PROPERTY_CATEGORY);
		}
		else
		{
			RemoveProperty(SI_PROPERTY_CATEGORY);
		}
	}

	// 获取维度
	std::string GetDim() const
	{
		return statdim_id;
	}

	std::string LogPrintInfo() const
	{
		std::string info;
		base::PubStr::SetFormatString(info, "VIRTUAL=[%d], "
											"CATEGORY=[%d], "
											"STAT_ID=[%s], "
											"STAT_NAME=[%s], "
											"DIM=[%s], "
											"PRIORITY=[%s], "
											"REPORT=[%s]", 
											IsVirtual(), 
											IsCategory(), 
											stat_id.c_str(), 
											stat_name.c_str(), 
											statdim_id.c_str(), 
											stat_priority.c_str(), 
											stat_report.c_str());
		return info;
	}

private:
	int         property;				// 属性
	std::string statdim_id;				// 统计维度ID

public:
	std::string stat_id;				// 统计指标ID
	std::string stat_name;				// 统计指标名称
	std::string stat_priority;			// 优先级别
	std::string stat_sql;				// 统计SQL
	std::string stat_report;			// 关联报表
};

// (业财稽核) 核对表因子结果信息
struct YCResult_HDB
{
public:
	YCResult_HDB(): stat_batch(0)
	{}

	static const int S_NUMBER_OF_MEMBERS = 7;			// 总成员数

public:
	// 导入数据
	bool Import(const std::vector<std::string>& vec_dat)
	{
		if ( vec_dat.size() != S_NUMBER_OF_MEMBERS )
		{
			return false;
		}

		int index = 0;
		stat_report = vec_dat[index++];
		stat_id     = vec_dat[index++];
		stat_name   = vec_dat[index++];
		statdim_id  = vec_dat[index++];
		stat_city   = vec_dat[index++];
	
		if ( !base::PubStr::Str2Int(vec_dat[index++], stat_batch) )
		{
			return false;
		}
	
		stat_value  = vec_dat[index++];
		return true;
	}

	// 导出数据
	void Export(std::vector<std::string>& vec_dat) const
	{
		std::vector<std::string> v_dat;
		v_dat.push_back(stat_report);
		v_dat.push_back(stat_id);
		v_dat.push_back(stat_name);
		v_dat.push_back(statdim_id);
		v_dat.push_back(stat_city);
		v_dat.push_back(base::PubStr::Int2Str(stat_batch));
		v_dat.push_back(stat_value);

		v_dat.swap(vec_dat);
	}

	std::string LogPrintInfo() const
	{
		std::string info;
		base::PubStr::SetFormatString(info, "REPORT=[%s], "
											"STAT_ID=[%s], "
											"STAT_NAME=[%s], "
											"DIM=[%s], "
											"CITY=[%s], "
											"BATCH=[%d], "
											"VALUE=[%s]", 
											stat_report.c_str(), 
											stat_id.c_str(), 
											stat_name.c_str(), 
											statdim_id.c_str(), 
											stat_city.c_str(), 
											stat_batch, 
											stat_value.c_str());
		return info;
	}

public:
	std::string stat_report;				// 关联报表
	std::string stat_id;					// 统计指标ID
	std::string stat_name;					// 统计指标名称
	std::string statdim_id;					// 统计维度ID
	std::string stat_city;					// 地市
	int         stat_batch;					// 批次
	std::string stat_value;					// 统计维度值
};

// (业财稽核) 日志信息
struct YCStatLog
{
public:
	YCStatLog(): stat_batch(0) {}

public:
	std::string stat_report;				// 业财报表
	int         stat_batch;					// 稽核批次
	std::string stat_datasource;			// 稽核数据源及批次
	std::string stat_city;					// 稽核地市
	std::string stat_cycle;					// 稽核账期
	std::string stat_time;					// 稽核时间
};

// (业财稽核) 数据源信息
struct YCSrcInfo
{
public:
	YCSrcInfo(): batch(0) {}

public:
	std::string src_tab;				// 数据源表名
	std::string field_period;			// 账期字段名
	std::string period;					// 账期
	std::string field_city;				// 地市字段名
	std::string city;					// 地市
	std::string field_batch;			// 批次字段名
	int         batch;					// 批次
};

// 地市详情表批次信息
struct YCXQBBatch
{
public:
	YCXQBBatch(): busi_batch(0) {}

	std::string LogPrintInfo() const
	{
		std::string info;
		base::PubStr::SetFormatString(info, "BILL_CYC=[%s], "
											"CITY=[%s], "
											"TYPE=[%s], "
											"BUSI_BATCH=[%d]", 
											bill_cyc.c_str(), 
											city.c_str(), 
											type.c_str(), 
											busi_batch);
		return info;
	}

public:
	std::string bill_cyc;				// 账期
	std::string city;					// 地市
	std::string type;					// 类型
	int         busi_batch;				// 业务版本号
};

// (业财稽核) 报表状态
struct YCReportState
{
public:
	std::string LogPrintInfo() const
	{
		std::string info;
		base::PubStr::SetFormatString(info, "REPORT_ID=[%s], "
											"BILL_CYC=[%s], "
											"CITY=[%s], "
											"STATUS=[%s], "
											"TYPE=[%s], "
											"ACTOR=[%s]", 
											report_id.c_str(), 
											bill_cyc.c_str(), 
											city.c_str(), 
											status.c_str(), 
											type.c_str(), 
											actor.c_str());
		return info;
	}

public:
	std::string report_id;				// 报表 ID
	std::string bill_cyc;				// 账期
	std::string city;					// 地市
	std::string status;					// 状态
	std::string type;					// 类型
	std::string actor;					// 角色
};

// (业财稽核) 流程记录日志
struct YCProcessLog
{
public:
	YCProcessLog(): version(0) {}

	std::string LogPrintInfo() const
	{
		std::string info;
		base::PubStr::SetFormatString(info, "REPORT_ID=[%s], "
											"BILL_CYC=[%s], "
											"CITY=[%s], "
											"STATUS=[%s], "
											"TYPE=[%s], "
											"ACTOR=[%s], "
											"OPER=[%s], "
											"VER=[%d], "
											"UPTIME=[%s]", 
											report_id.c_str(), 
											bill_cyc.c_str(), 
											city.c_str(), 
											status.c_str(), 
											type.c_str(), 
											actor.c_str(), 
											oper.c_str(), 
											version, 
											uptime.c_str());
		return info;
	}

public:
	std::string report_id;				// 报表 ID
	std::string bill_cyc;				// 账期
	std::string city;					// 地市
	std::string status;					// 状态
	std::string type;					// 类型
	std::string actor;					// 角色
	std::string oper;					// 操作员
	int         version;				// 版本号
	std::string uptime;					// 更新时间
};

// 详情表（业务侧）更新字段
struct UpdateFields_YW
{
public:
	UpdateFields_YW(): last_batch(0)
	{}

public:
	std::vector<std::string> dim_upd_fields;			// DIM更新字段名列表
	std::vector<std::string> val_upd_fields;			// VAL更新字段名列表
	std::string fld_billcyc;							// 详情表账期字段名
	std::string fld_city;								// 详情表地市字段名
	std::string fld_batch;								// 详情表批次字段名
	std::string fld_dim;								// 详情表维度字段名
	int         last_batch;								// 上一批次
};

