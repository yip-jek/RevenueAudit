#pragma once

#include <set>
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

	//// 获取采集规则A和B的字段SQL (外部保证正确性)
	//// inverse为true时，表示A和B的所属表反转
	//static std::string GetCompareFields(OneEtlRule& rule_A, OneEtlRule& rule_B, bool inverse = false);

	// 获取采集规则A和B的维度SQL (外部保证正确性)
	static std::string GetCompareDims(OneEtlRule& rule_A, OneEtlRule& rule_B);

	// 获取采集规则A和B的相等形式的值SQL (外部保证正确性)
	static std::string GetCompareEqualVals(OneEtlRule& rule_A, OneEtlRule& rule_B);

	// 获取采集规则A和B的不等形式的值SQL (外部保证正确性)
	static std::string GetCompareUnequalVals(OneEtlRule& rule_A, OneEtlRule& rule_B);

	// 获取指定列的采集规则A和B的字段SQL (外部保证正确性)
	// 若没有指定列集（即参数 vec_col 为空），则默认所有列
	static std::string GetCompareFieldsByCol(OneEtlRule& rule_A, OneEtlRule& rule_B, std::vector<int>& vec_col);

	// 获取指定列的采集规则A和B的相等形式的值SQL (外部保证正确性)
	static std::string GetCompareEqualValsByCol(OneEtlRule& rule_A, OneEtlRule& rule_B, std::vector<int>& vec_col);

	// 获取指定列的采集规则A和B的不等形式的值SQL (外部保证正确性)
	static std::string GetCompareUnequalValsByCol(OneEtlRule& rule_A, OneEtlRule& rule_B, std::vector<int>& vec_col);

	// 获取采集规则的字段SQL (外部保证正确性)
	// tab_prefix为字段前置，例如："a.", "b."等
	static void GetOneRuleFields(std::string& dim_sql, std::string& val_sql, OneEtlRule& rule, bool set_as, const std::string& tab_prefix = std::string());

	//// 获取采集规则的 <NULL> 值SQL (外部保证正确性)
	//static std::string GetOneRuleValsNull(OneEtlRule& rule, const std::string& tab_prefix);

	// 获取采集规则的统计SQL集 (外部保证正确性)
	static void GetEtlStatisticsSQLs(std::vector<OneEtlRule>& vec_rules, std::vector<std::string>& vec_hivesql, bool union_all);

	// 获取指定组的采集规则的统计SQL集 (外部保证正确性)
	static void GetEtlStatisticsSQLsBySet(std::vector<OneEtlRule>& vec_rules, std::set<int>& set_int, std::vector<std::string>& vec_hivesql, bool union_all);

	//// 获取直接分析表达式
	//static std::string GetStraightAnaCondition(AnalyseRule::AnalyseConditionType cond_type, const std::string& condition, bool add);

	//// 将SQL条件子串插入到SQL语句中
	//static void AddConditionSql(std::string& src_sql, const std::string& cond_sql);
};

