#pragma once

#include <map>
#include <string>
#include <vector>
#include "pubstr.h"

// 单个维度信息
class OneEtlDim
{
public:
	// 维度备注类型
	enum DIM_MEMO_TYPE
	{
		DMTYPE_NONE			= 0,			// 无（即没有指定类型）
		DMTYPE_JOIN_ON		= 1,			// 外连-关联类型
		DMTYPE_JOIN_DIM		= 2,			// 外连-维度类型
		DMTYPE_SHOW_UP		= 3,			// 显示类型（用于单独显示的维度）
	};

public:
	OneEtlDim();

public:
	// 设置维度备注类型
	void SetDimMemoType(const std::string& m_type);

	// 维度备注类型的字符串表示
	std::string GetDimMemoTypeStr() const;

public:
	std::string   EtlDimID;						// 采集维度ID
	int           EtlDimSeq;					// 采集维度序号
	std::string   EtlDimName;					// 采集维度名称
	std::string   EtlDimDesc;					// 采集维度描述
	std::string   EtlDimSrcName;				// 维度对应源字段名称
	DIM_MEMO_TYPE EtlDimMemo;					// 维度备注类型
};

// 采集维度信息
class AcqEtlDim
{
public:
	AcqEtlDim(): isValid(false)
	{}

public:
	bool                   isValid;					// 是否有效
	std::string            acqEtlDimID;				// 采集维度ID
	std::vector<OneEtlDim> vecEtlDim;				// 维度信息集
};

// 单个采集值信息
class OneEtlVal
{
public:
	// 值备注类型
	enum VAL_MEMO_TYPE
	{
		VMTYPE_NONE		= 0,			// 无（即没有指定类型）
		VMTYPE_JOIN_VAL	= 1,			// 外连-值类型
		//VMTYPE_SHOW_UP	= 2,			// 显示类型（用于单独显示的值）
	};

public:
	OneEtlVal();

public:
	// 设置值备注类型
	void SetValMemoType(const std::string& m_type);

	// 值备注类型的字符串表示
	std::string GetValMemoTypeStr() const;

public:
	std::string   EtlValID;						// 采集值ID
	int           EtlValSeq;					// 采集值序号
	std::string   EtlValName;					// 采集值名称
	std::string   EtlValDesc;					// 采集值描述
	std::string   EtlValSrcName;				// 值对应源字段名称
	VAL_MEMO_TYPE EtlValMemo;					// 值备注类型
};

// 采集值信息
class AcqEtlVal
{
public:
	AcqEtlVal(): isValid(false)
	{}

public:
	bool                   isValid;					// 是否有效
	std::string            acqEtlValID;				// 采集值ID
	std::vector<OneEtlVal> vecEtlVal;				// 值信息集
};

// 采集数据源信息
class EtlSrcInfo
{
public:
	// 数据源条件类型
	enum EtlSrcType
	{
		ESTYPE_UNKNOWN		= 0,		// 未知类型
		ESTYPE_STRAIGHT		= 1,		// 直接条件
	};

public:
	EtlSrcInfo();

public:
	// 设置数据源条件类型
	bool SetEtlSrcType(const std::string& type);

public:
	EtlSrcType  src_type;			// 数据源条件类型
	std::string condition;			// 条件 SQL 语句
};

// 数据源
class DataSource
{
public:
	DataSource(): isValid(false)
	{}

public:
	bool        isValid;			// 是否有效
	std::string srcTabName;			// 数据源表名
};

// 采集规则任务信息
class AcqTaskInfo
{
public:
	// 采集条件类型
	enum EtlConditionType
	{
		ETLCTYPE_UNKNOWN              = 0,			// 未知类型
		ETLCTYPE_NONE                 = 1,			// 不带条件
		ETLCTYPE_STRAIGHT             = 2,			// 直接条件
		ETLCTYPE_OUTER_JOIN           = 3,			// 外连条件
		ETLCTYPE_OUTER_JOIN_WITH_COND = 4,			// 外连加条件

		// 话单稽核
		ETLCTYPE_HDJH_HIVE_SQLS = 101,				// 话单稽核的采集条件类型
	};

	// 数据源类型
	enum DataSourceType
	{
		DSTYPE_UNKNOWN	= 0,			// 未知类型
		DSTYPE_HIVE		= 1,			// HIVE类型
		DSTYPE_DB2		= 2,			// DB2类型
	};

public:
	AcqTaskInfo();

public:
	// 设置采集条件类型
	bool SetConditionType(const std::string& type);

	// 设置数据源类型
	bool SetDataSourceType(const std::string& type);

public:
	std::string      EtlRuleID;						// 采集规则ID
	std::string      KpiID;							// 指标ID
	std::string      EtlRuleTime;					// 采集时间(周期)
	std::string      EtlRuleType;					// 采集处理方式
	std::string      EtlRuleTarget;					// 采集目标数据存放
	std::string      EtlCondition;					// 采集条件
	EtlConditionType EtlCondType;					// 采集条件类型
	DataSourceType   DataSrcType;					// 数据源类型

public:
	std::vector<DataSource>   vecEtlRuleDataSrc;				// 采集数据源
	std::vector<AcqEtlDim>    vecEtlRuleDim;					// 采集维度信息
	std::vector<AcqEtlVal>    vecEtlRuleVal;					// 采集值信息
	std::map<int, EtlSrcInfo> mapEtlSrc;						// 采集数据源信息
};

// 业财稽核因子规则信息
class YCInfo
{
public:
	static const char* const S_EXTERNAL_SQL;			// 外部SQL

	// 是否为外部SQL
	static bool IsExternalSQL(const std::string& sql)
	{
		const std::string C_SQL = base::PubStr::TrimUpperB(sql);
		const size_t EXTERNAL_SIZE = std::string(S_EXTERNAL_SQL).size();

		return (C_SQL.substr(0, EXTERNAL_SIZE) == S_EXTERNAL_SQL);
	}

public:
	void Clear()
	{
		stat_dimid.clear();
		std::vector<std::string>().swap(vec_statsql);
	}

public:
	std::string              stat_dimid;			// 统计维度ID
	std::vector<std::string> vec_statsql;			// 统计SQL
};

// (业财稽核) 任务请求
struct YCTaskRequest
{
public:
	YCTaskRequest(): seq(0)
	{}

	std::string LogPrintInfo() const
	{
		std::string info;
		base::PubStr::SetFormatString(info, "SEQ=[%d], "
											"BILL_CYC=[%s], "
											"CITY=[%s], "
											"ACTOR=[%s], "
											"OPER=[%s]", 
											seq, 
											bill_cyc.c_str(), 
											city.c_str(), 
											actor.c_str(), 
											oper.c_str());
		return info;
	}

public:
	int         seq;					// 任务流水号
	std::string kpi_id;					// 指标ID
	std::string stat_id;				// 报表ID
	std::string bill_cyc;				// 任务账期
	std::string city;					// 任务地市
	std::string actor;					// 角色
	std::string oper;					// 操作员
	std::string task_state;				// 任务状态
	std::string state_desc;				// 任务状态描述
	std::string task_desc;				// 任务备注
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

