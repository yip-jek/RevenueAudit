#pragma once

#include "anataskinfo.h"

// 分析任务信息的工具类
class TaskInfoUtil
{
public:
	// 检查前两个采集规则
	// 返回值: 0-成功, -1-少于两个采集规则, -2-维度不一致, -3-值不一致
	static int CheckDualEtlRule(std::vector<OneEtlRule>& vec_etlrule);

	// 检查多个采集规则 (支持 1 到 N 个采集规则)
	// 返回值: 0-成功, -1-没有采集规则, -2-维度不一致, -3-值不一致
	static int CheckPluralEtlRule(std::vector<OneEtlRule>& vec_etlrule);

	// 获取对比类型的字段SQL (外部保证正确性)
	static std::string GetCompareFields(OneEtlRule& rule_A, OneEtlRule& rule_B);

	// 获取对比类型的维度SQL (外部保证正确性)
	static std::string GetCompareDims(OneEtlRule& rule_A, OneEtlRule& rule_B);

	// 获取对比类型的相等形式的值SQL (外部保证正确性)
	static std::string GetCompareEqualVals(OneEtlRule& rule_A, OneEtlRule& rule_B);

	// 获取对比类型的不等形式的值SQL (外部保证正确性)
	static std::string GetCompareUnequalVals(OneEtlRule& rule_A, OneEtlRule& rule_B);
};

