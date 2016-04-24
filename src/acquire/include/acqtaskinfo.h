#pragma once

#include <string>
#include <vector>

// 采集维度信息
struct AcqEtlDim
{
public:
	AcqEtlDim(): EtlDimID(0), EtlDimSeq(0)
	{}

	AcqEtlDim(const AcqEtlDim& dim)
	:EtlDimID(dim.EtlDimID)
	,EtlDimSeq(dim.EtlDimSeq)
	,EtlDimName(dim.EtlDimName)
	,EtlDimDesc(dim.EtlDimDesc)
	,EtlDimSrcName(dim.EtlDimSrcName)
	{}

	const AcqEtlDim& operator = (const AcqEtlDim& dim)
	{
		if ( this != &dim )
		{
			this->EtlDimID      = dim.EtlDimID;
			this->EtlDimSeq     = dim.EtlDimSeq;
			this->EtlDimName    = dim.EtlDimName;
			this->EtlDimDesc    = dim.EtlDimDesc;
			this->EtlDimSrcName = dim.EtlDimSrcName;
		}

		return *this;
	}

public:
	int			EtlDimID;					// 采集维度ID
	int			EtlDimSeq;					// 采集维度序号
	std::string	EtlDimName;					// 采集维度名称
	std::string	EtlDimDesc;					// 采集维度描述
	std::string	EtlDimSrcName;				// 维度对应源字段名称
};

// 采集值信息
struct AcqEtlVal
{
public:
	AcqEtlVal(): EtlValID(0), EtlValSeq(0)
	{}

	AcqEtlVal(const AcqEtlVal& val)
	:EtlValID(val.EtlValID)
	,EtlValSeq(val.EtlValSeq)
	,EtlValName(val.EtlValName)
	,EtlValDesc(val.EtlValDesc)
	,EtlValSrcName(val.EtlValSrcName)
	{}

	const AcqEtlVal& operator = (const AcqEtlVal& val)
	{
		if ( this != &val )
		{
			this->EtlValID      = val.EtlValID;
			this->EtlValSeq     = val.EtlValSeq;
			this->EtlValName    = val.EtlValName;
			this->EtlValDesc    = val.EtlValDesc;
			this->EtlValSrcName = val.EtlValSrcName;
		}

		return *this;
	}

public:
	int			EtlValID;					// 采集值ID
	int			EtlValSeq;					// 采集值序号
	std::string	EtlValName;					// 采集值名称
	std::string	EtlValDesc;					// 采集值描述
	std::string	EtlValSrcName;				// 值对应源字段名称
};

// 采集规则任务信息
struct AcqTaskInfo
{
public:
	AcqTaskInfo(): EtlRuleID(0), KpiID(0)
	{}

	AcqTaskInfo(const AcqTaskInfo& info)
	:EtlRuleID(info.EtlRuleID)
	,KpiID(info.KpiID)
	,EtlRuleTime(info.EtlRuleTime)
	,EtlRuleType(info.EtlRuleType)
	,EtlRuleTarget(info.EtlRuleTarget)
	{
		vecEtlRuleDataSrc.insert(vecEtlRuleDataSrc.begin(), info.vecEtlRuleDataSrc.begin(), info.vecEtlRuleDataSrc.end());
	}

public:
	int			EtlRuleID;					// 采集规则ID
	int			KpiID;						// 指标ID
	std::string	EtlRuleTime;				// 采集时间(周期)
	std::string	EtlRuleType;				// 采集处理方式
	std::string	EtlRuleTarget;				// 采集目标数据存放

	std::vector<std::string>	vecEtlRuleDataSrc;			// 采集数据源
	std::vector<AcqEtlDim>		vecEtlRuleDim;				// 采集维度信息
	std::vector<AcqEtlVal>		vecEtlRuleVal;				// 采集值信息
};

