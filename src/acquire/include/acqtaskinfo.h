#pragma once

#include <string>
#include <vector>

// 采集维度信息
struct AcqEtlDim
{
	int			EtlDimID;
	int			EtlDimSeq;
	std::string	EtlDimName;
	std::string	EtlDimDesc;
	std::string	EtlDimSrcName;
};

// 采集值信息
struct AcqEtlVal
{
	int			EtlValID;
	int			EtlValSeq;
	std::string	EtlValName;
	std::string	EtlValDesc;
	std::string	EtlValSrcName;
};

// 采集规则任务信息
struct AcqTaskInfo
{
	int			EtlRuleID;
	int			KpiID;
	std::string	EtlRuleTime;
	std::string	EtlRuleType;
	std::string	EtlRuleTarget;

	std::vector<std::string>	vecEtlRuleDataSrc;
	std::vector<AcqEtlDim>		vecEtlRuleDim;
	std::vector<AcqEtlVal>		vecEtlRuleVal;
};

