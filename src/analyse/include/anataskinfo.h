#pragma once

#include <string>
#include <vector>

struct OneEtlRule
{
public:
	OneEtlRule(): EtlRuleID(0)
	{}

	OneEtlRule(const OneEtlRule& one): EtlRuleID(one.EtlRuleID), TargetPatch(one.TargetPatch)
	{}

	const OneEtlRule& operator = (const OneEtlRule& one)
	{
		if ( this != &one )
		{
			this->EtlRuleID   = one.EtlRuleID;
			this->TargetPatch = one.TargetPatch;
		}

		return *this;
	}

public:
	int			EtlRuleID;
	std::string	TargetPatch;
};

struct KpiColumn
{
public:

public:
	int			KpiID;
	int			ColType;
	int			ColSeq;
	std::string DBName;
};

struct DimVal
{
public:

public:
	int			KpiID;
	std::string DBName;
	std::string DimVal;
};

struct AnalyseRule
{
public:

public:
	int			AnaID;
	std::string AnaName;
	std::string AnaType;
	std::string AnaExpress;
};

struct AnaTaskInfo
{
public:
	AnaTaskInfo()
	{}

public:
	int			KpiID;
	std::string KpiCycle;
	int			AnaID;
	int			AlarmID;
	std::string ResultType;
	std::string TableName;
	AnalyseRule ;

	std::vector<OneEtlRule>		vecEtlRule;
	std::vector<KpiColumn>		vecKpiCol;
};

