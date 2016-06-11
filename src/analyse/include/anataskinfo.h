#pragma once

#include <string>
#include <vector>
#include "pubstr.h"

// 单个采集维度信息
struct OneEtlDim
{
public:
	OneEtlDim(): EtlDimSeq(0)
	{}

	OneEtlDim(const OneEtlDim& dim)
	:EtlDimID(dim.EtlDimID)
	,EtlDimSeq(dim.EtlDimSeq)
	,EtlDimName(dim.EtlDimName)
	{}

public:
	const OneEtlDim& operator = (const OneEtlDim& dim)
	{
		if ( this != &dim )
		{
			this->EtlDimID      = dim.EtlDimID;
			this->EtlDimSeq     = dim.EtlDimSeq;
			this->EtlDimName    = dim.EtlDimName;
		}

		return *this;
	}

public:
	std::string	EtlDimID;					// 采集维度ID
	int			EtlDimSeq;					// 采集维度序号
	std::string	EtlDimName;					// 采集维度名称
};

// 单个采集值信息
struct OneEtlVal
{
public:
	OneEtlVal(): EtlValSeq(0)
	{}

	OneEtlVal(const OneEtlVal& val)
	:EtlValID(val.EtlValID)
	,EtlValSeq(val.EtlValSeq)
	,EtlValName(val.EtlValName)
	{}

public:
	const OneEtlVal& operator = (const OneEtlVal& val)
	{
		if ( this != &val )
		{
			this->EtlValID      = val.EtlValID;
			this->EtlValSeq     = val.EtlValSeq;
			this->EtlValName    = val.EtlValName;
		}

		return *this;
	}

public:
	std::string	EtlValID;					// 采集值ID
	int			EtlValSeq;					// 采集值序号
	std::string	EtlValName;					// 采集值名称
};

// 单个采集规则
struct OneEtlRule
{
public:
	OneEtlRule()
	{}

	OneEtlRule(const OneEtlRule& one)
	:EtlRuleID(one.EtlRuleID)
	,KpiID(one.KpiID)
	,EtlTime(one.EtlTime)
	,TargetPatch(one.TargetPatch)
	,DimID(one.DimID)
	,ValID(one.ValID)
	,vecEtlDim(one.vecEtlDim)
	,vecEtlVal(one.vecEtlVal)
	{}

	const OneEtlRule& operator = (const OneEtlRule& one)
	{
		if ( this != &one )
		{
			this->EtlRuleID   = one.EtlRuleID;
			this->KpiID       = one.KpiID;
			this->EtlTime     = one.EtlTime;
			this->TargetPatch = one.TargetPatch;
			this->DimID       = one.DimID;
			this->ValID       = one.ValID;
			this->vecEtlDim   = one.vecEtlDim;
			this->vecEtlVal   = one.vecEtlVal;
		}

		return *this;
	}

public:
	std::string	EtlRuleID;			// 采集规则ID
	std::string	KpiID;				// 指标ID
	std::string EtlTime;			// 采集时间表达式
	std::string	TargetPatch;		// 采集目标数据表
	std::string DimID;				// 采集维度规则ID
	std::string ValID;				// 采集值规则ID

	std::vector<OneEtlDim>	vecEtlDim;			// 采集维度信息集
	std::vector<OneEtlVal>	vecEtlVal;			// 采集值信息集
};

// 指标字段
struct KpiColumn
{
public:
	// 字段类型
	enum ColumnType
	{
		CTYPE_UNKNOWN	= 0,		// 未知类型
		CTYPE_DIM		= 1,		// 维度类型
		CTYPE_VAL		= 2,		// 值类型
	};

	// 前台显示方式
	enum DisplayType
	{
		DTYPE_UNKNOWN	= 0,		// 未知显示方式
		DTYPE_NULL		= 1,		// 不显示方式
		DTYPE_LIST		= 2,		// 列表显示方式
		DTYPE_TEXT		= 3,		// 文件显示方式
	};

public:
	KpiColumn(): ColType(CTYPE_UNKNOWN), ColSeq(0), DisType(DTYPE_UNKNOWN)
	{}

	KpiColumn(const KpiColumn& col)
	:KpiID(col.KpiID)
	,ColType(col.ColType)
	,ColSeq(col.ColSeq)
	,DBName(col.DBName)
	,CNName(col.CNName)
	,DisType(col.DisType)
	{}

	const KpiColumn& operator = (const KpiColumn& col)
	{
		if ( this != &col )
		{
			this->KpiID   = col.KpiID  ;
			this->ColType = col.ColType;
			this->ColSeq  = col.ColSeq ;
			this->DBName  = col.DBName ;
			this->CNName  = col.CNName ;
			this->DisType = col.DisType;
		}

		return *this;
	}

public:
	// 设置字段类型
	bool SetColumnType(const std::string& c_type)
	{
		const std::string TYPE = base::PubStr::TrimUpperB(c_type);

		if ( "DIM" == TYPE )
		{
			ColType = CTYPE_DIM;
		}
		else if ( "VAL" == TYPE )
		{
			ColType = CTYPE_VAL;
		}
		else
		{
			ColType = CTYPE_UNKNOWN;
			return false;
		}

		return true;
	}

	// 设置前台显示方式
	bool SetDisplayType(const std::string& d_type)
	{
		const std::string TYPE = base::PubStr::TrimUpperB(d_type);

		if ( "NULL" == TYPE )
		{
			DisType = DTYPE_NULL;
		}
		else if ( "LIST" == TYPE )
		{
			DisType = DTYPE_LIST;
		}
		else if ( "TEXT" == TYPE )
		{
			DisType = DTYPE_TEXT;
		}
		else
		{
			DisType = DTYPE_UNKNOWN;
			return false;
		}

		return true;
	}

public:
	std::string	KpiID;				// 指标ID
	ColumnType	ColType;			// 字段类型
	int			ColSeq;				// 字段序号
	std::string DBName;				// 字段名称
	std::string CNName;				// 字段中文名称
	DisplayType	DisType;			// 前台显示方式
};

// 维度取值
struct DimVal
{
public:
	DimVal()
	{}

	DimVal(const DimVal& d_v)
	:KpiID(d_v.KpiID)
	,DBName(d_v.DBName)
	,Value(d_v.Value)
	,CNName(d_v.CNName)
	{}

	const DimVal& operator = (const DimVal& d_v)
	{
		if ( this != &d_v )
		{
			this->KpiID  = d_v.KpiID ;
			this->DBName = d_v.DBName;
			this->Value = d_v.Value;
			this->CNName = d_v.CNName;
		}

		return *this;
	}

public:
	std::string	KpiID;			// 指标ID
	std::string DBName;			// 维度字段名称
	std::string Value;			// 维度取值
	std::string CNName;			// 维度取值中文名称
};

// 分析规则
struct AnalyseRule
{
public:
	// 分析规则类型
	enum AnalyseType
	{
		ANATYPE_UNKNOWN				= 0,			// 未知类型
		ANATYPE_SUMMARY_COMPARE		= 1,			// 汇总对比
		ANATYPE_DETAIL_COMPARE		= 2,			// 明细对比
		ANATYPE_STATISTICS			= 3,			// 一般统计
		ANATYPE_REPORT_STATISTICS	= 4,			// 报表统计

		// 可执行的HIVE SQL语句
		// 分析规则表达式即是可执行的HIVE SQL语句
		ANATYPE_HIVE_SQL	= 99
	};

	//// 分析条件类型
	//enum AnalyseConditionType
	//{
	//	ACTYPE_UNKNOWN		= 0,		// 未知类型
	//	ACTYPE_NONE			= 1,		// 无条件
	//	ACTYPE_STRAIGHT		= 2,		// 直接条件
	//};

public:
	//AnalyseRule(): AnaType(ANATYPE_UNKNOWN), AnaCondType(ACTYPE_UNKNOWN)
	AnalyseRule(): AnaType(ANATYPE_UNKNOWN)
	{}

	AnalyseRule(const AnalyseRule& ana)
	:AnaID(ana.AnaID)
	,AnaName(ana.AnaName)
	,AnaType(ana.AnaType)
	,AnaExpress(ana.AnaExpress)
	//,AnaCondType(ana.AnaCondType)
	//,AnaCondition(ana.AnaCondition)
	{}

	const AnalyseRule& operator = (const AnalyseRule& ana)
	{
		if ( this != &ana )
		{
			this->AnaID        = ana.AnaID     ;
			this->AnaName      = ana.AnaName   ;
			this->AnaType      = ana.AnaType   ;
			this->AnaExpress   = ana.AnaExpress;
			//this->AnaCondType  = ana.AnaCondType;
			//this->AnaCondition = ana.AnaCondition;
		}

		return *this;
	}

public:
	// 设置分析规则类型
	bool SetAnalyseType(const std::string& a_type)
	{
		const std::string TYPE = base::PubStr::TrimUpperB(a_type);

		if ( "SUMMARY" == TYPE )		// 汇总对比
		{
			AnaType = ANATYPE_SUMMARY_COMPARE;
		}
		else if ( "DETAIL" == TYPE )		// 明细对比
		{
			AnaType = ANATYPE_DETAIL_COMPARE;
		}
		else if ( "STATISTICS" == TYPE )	// 一般统计
		{
			AnaType = ANATYPE_STATISTICS;
		}
		else if ( "REPORT_STATISTICS" == TYPE )		// 报表统计
		{
			AnaType = ANATYPE_REPORT_STATISTICS;
		}
		else if ( "HIVE_SQL" == TYPE )		// 可执行的HIVE SQL语句
		{
			AnaType = ANATYPE_HIVE_SQL;
		}
		else		// 未知类型
		{
			AnaType = ANATYPE_UNKNOWN;
			return false;
		}

		return true;
	}

	//// 设置分析条件类型
	//bool SetAnalyseConditionType(const std::string& type)
	//{
	//	const std::string COND_TYPE = base::PubStr::TrimUpperB(type);

	//	if ( "NONE" == COND_TYPE )				// 无条件
	//	{
	//		AnaCondType = ACTYPE_NONE;
	//	}
	//	else if ( "STRAIGHT" == COND_TYPE )		// 直接条件
	//	{
	//		AnaCondType = ACTYPE_STRAIGHT;
	//	}
	//	else	// 未知类型
	//	{
	//		AnaCondType = ACTYPE_UNKNOWN;
	//		return false;
	//	}

	//	return true;
	//}

public:
	std::string				AnaID;				// 分析规则ID
	std::string 			AnaName;			// 分析规则名称
	AnalyseType 			AnaType;			// 分析规则类型
	std::string 			AnaExpress;			// 分析规则表达式
	//AnalyseConditionType	AnaCondType;		// 分析条件类型
	//std::string				AnaCondition;		// 分析条件表达式
};

// 告警规则
struct AlarmRule
{
public:
	// 告警类型
	enum ALARM_TYPE
	{
		AT_UNKNOWN		= 0,		// 未知类型
		AT_FLUCTUATE	= 1,		// 波动告警
		AT_RATIO		= 2,		// 对比告警
	};

public:
	AlarmRule(): AlarmType(AT_UNKNOWN)
	{}

	AlarmRule(const AlarmRule& alarm)
	:AlarmID(alarm.AlarmID)
	,AlarmName(alarm.AlarmName)
	,AlarmType(alarm.AlarmType)
	,AlarmExpress(alarm.AlarmExpress)
	,AlarmEvent(alarm.AlarmEvent)
	,SendAms(alarm.SendAms)
	,SendSms(alarm.SendSms)
	{}

	const AlarmRule& operator = (const AlarmRule& alarm)
	{
		if ( this != &alarm )
		{
			this->AlarmID      = alarm.AlarmID     ;
			this->AlarmName    = alarm.AlarmName   ;
			this->AlarmType    = alarm.AlarmType   ;
			this->AlarmExpress = alarm.AlarmExpress;
			this->AlarmEvent   = alarm.AlarmEvent  ;
			this->SendAms      = alarm.SendAms     ;
			this->SendSms      = alarm.SendSms     ;
		}

		return *this;
	}

	// 设置告警类型
	bool SetAlarmType(const std::string& a_type)
	{
		const std::string TYPE = base::PubStr::TrimUpperB(a_type);

		if ( "FLUCTUATE" == TYPE )		// 波动告警
		{
			AlarmType = AT_FLUCTUATE;
		}
		else if ( "RATIO" == TYPE )		// 对比告警
		{
			AlarmType = AT_RATIO;
		}
		else	// 未知告警
		{
			AlarmType = AT_UNKNOWN;
			return false;
		}

		return true;
	};

public:
	std::string	AlarmID;				// 告警规则ID
	std::string	AlarmName;				// 告警规则名称
	ALARM_TYPE	AlarmType;				// 告警规则类型
	std::string	AlarmExpress;			// 告警规则表达式
	std::string	AlarmEvent;				// 告警事件
	std::string	SendAms;				// 发送工单
	std::string	SendSms;				// 发送短信
};

// 指标任务信息
struct AnaTaskInfo
{
public:
	// 结果表类型
	enum ResultTableType
	{
		TABTYPE_UNKNOWN		= 0,		// 未知类型
		TABTYPE_COMMON		= 1,		// 普通表
		TABTYPE_DAY			= 2,		// 天表
		TABTYPE_MONTH		= 3,		// 月表
		TABTYPE_YEAR		= 4,		// 年表
	};

public:
	AnaTaskInfo(): ResultType(TABTYPE_UNKNOWN)
	{}

	AnaTaskInfo(const AnaTaskInfo& info)
	:KpiID(info.KpiID)
	,DataSrcType(info.DataSrcType)
	,KpiCycle(info.KpiCycle)
	,ResultType(info.ResultType)
	,TableName(info.TableName)
	,AnaRule(info.AnaRule)
	,vecAlarm(info.vecAlarm)
	,vecEtlRule(info.vecEtlRule)
	,vecKpiDimCol(info.vecKpiDimCol)
	,vecKpiValCol(info.vecKpiValCol)
	{
		//this->vecAlarm.insert(this->vecAlarm.begin(), info.vecAlarm.begin(), info.vecAlarm.end());
		//this->vecEtlRule.insert(this->vecEtlRule.begin(), info.vecEtlRule.begin(), info.vecEtlRule.end());
		//this->vecKpiDimCol.insert(this->vecKpiDimCol.begin(), info.vecKpiDimCol.begin(), info.vecKpiDimCol.end());
		//this->vecKpiValCol.insert(this->vecKpiValCol.begin(), info.vecKpiValCol.begin(), info.vecKpiValCol.end());
	}

	const AnaTaskInfo& operator = (const AnaTaskInfo& info)
	{
		if ( this != &info )
		{
			this->KpiID       = info.KpiID     ;
			this->DataSrcType = info.DataSrcType;
			this->KpiCycle    = info.KpiCycle  ;
			this->ResultType  = info.ResultType;
			this->TableName   = info.TableName ;
			this->AnaRule     = info.AnaRule   ;

			//this->vecAlarm.insert(this->vecAlarm.begin(), info.vecAlarm.begin(), info.vecAlarm.end());
			//this->vecEtlRule.insert(this->vecEtlRule.begin(), info.vecEtlRule.begin(), info.vecEtlRule.end());
			//this->vecKpiDimCol.insert(this->vecKpiDimCol.begin(), info.vecKpiDimCol.begin(), info.vecKpiDimCol.end());
			//this->vecKpiValCol.insert(this->vecKpiValCol.begin(), info.vecKpiValCol.begin(), info.vecKpiValCol.end());
			this->vecAlarm     = info.vecAlarm;
			this->vecEtlRule   = info.vecEtlRule;
			this->vecKpiDimCol = info.vecKpiDimCol;
			this->vecKpiValCol = info.vecKpiValCol;
		}

		return *this;
	}

public:
	// 设置结果表类型
	bool SetTableType(const std::string& t_type)
	{
		const std::string TYPE = base::PubStr::TrimUpperB(t_type);

		if ( "COMMON_TABLE" == TYPE )
		{
			ResultType = TABTYPE_COMMON;
		}
		else if ( "DAY_TABLE" == TYPE )
		{
			ResultType = TABTYPE_DAY;
		}
		else if ( "MONTH_TABLE" == TYPE )
		{
			ResultType = TABTYPE_MONTH;
		}
		else if ( "YEAR_TABLE" == TYPE )
		{
			ResultType = TABTYPE_YEAR;
		}
		else
		{
			ResultType = TABTYPE_UNKNOWN;
			return false;
		}

		return true;
	}

public:
	std::string		KpiID;					// 指标ID
	std::string		DataSrcType;			// 数据源类型
	std::string		KpiCycle;				// 指标周期
	ResultTableType	ResultType;				// 结果表类型
	std::string		TableName;				// 分析结果表
	AnalyseRule		AnaRule;				// 分析规则

	std::vector<AlarmRule>		vecAlarm;			// 告警规则集
	std::vector<OneEtlRule>		vecEtlRule;			// 采集规则集
	std::vector<KpiColumn>		vecKpiDimCol;		// 指标维度字段集
	std::vector<KpiColumn>		vecKpiValCol;		// 指标值字段集
};

