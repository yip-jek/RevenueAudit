#pragma once

#include <string>
#include <vector>

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
	KpiColumn(): ColSeq(0)
	{}

	KpiColumn(const KpiColumn& col)
	:KpiID(col.KpiID)
	,ColType(col.ColType)
	,ColSeq(col.ColSeq)
	,DBName(col.DBName)
	,CNName(col.CNName)
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
		}

		return *this;
	}

public:
	std::string	KpiID;				// 指标ID
	std::string	ColType;			// 字段类型
	int			ColSeq;				// 字段序号
	std::string DBName;				// 字段名称
	std::string CNName;				// 字段中文名称
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
	AnalyseRule()
	{}

	AnalyseRule(const AnalyseRule& ana)
	:AnaID(ana.AnaID)
	,AnaName(ana.AnaName)
	,AnaType(ana.AnaType)
	,AnaExpress(ana.AnaExpress)
	{}

	const AnalyseRule& operator = (const AnalyseRule& ana)
	{
		if ( this != &ana )
		{
			this->AnaID      = ana.AnaID     ;
			this->AnaName    = ana.AnaName   ;
			this->AnaType    = ana.AnaType   ;
			this->AnaExpress = ana.AnaExpress;
		}

		return *this;
	}

public:
	std::string	AnaID;				// 分析规则ID
	std::string AnaName;			// 分析规则名称
	std::string AnaType;			// 分析类型
	std::string AnaExpress;			// 分析表达式
};

// 告警规则
struct AlarmRule
{
public:
	AlarmRule()
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

public:
	std::string	AlarmID;				// 告警规则ID
	std::string	AlarmName;				// 告警规则名称
	std::string	AlarmType;				// 告警规则类型
	std::string	AlarmExpress;			// 告警规则表达式
	std::string	AlarmEvent;				// 告警事件
	std::string	SendAms;				// 发送工单
	std::string	SendSms;				// 发送短信
};

// 指标任务信息
struct AnaTaskInfo
{
public:
	AnaTaskInfo()
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
	std::string	KpiID;					// 指标ID
	std::string	DataSrcType;			// 数据源类型
	std::string	KpiCycle;				// 指标周期
	std::string	ResultType;				// 结果表类型
	std::string	TableName;				// 分析结果表
	AnalyseRule	AnaRule;				// 分析规则

	std::vector<AlarmRule>		vecAlarm;			// 告警规则集
	std::vector<OneEtlRule>		vecEtlRule;			// 采集规则集
	std::vector<KpiColumn>		vecKpiDimCol;		// 指标维度字段集
	std::vector<KpiColumn>		vecKpiValCol;		// 指标值字段集
};

