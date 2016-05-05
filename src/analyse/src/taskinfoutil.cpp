#include "taskinfoutil.h"

int TaskInfoUtil::CheckDualEtlRule(std::vector<OneEtlRule>& vec_etlrule)
{
	if ( vec_etlrule.size() < 2 )
	{
		return -1;
	}

	OneEtlRule& rule_0 = vec_etlrule[0];
	OneEtlRule& rule_1 = vec_etlrule[1];
	if ( rule_0.vecEtlDim.size() != rule_1.vecEtlDim.size() )
	{
		return -2;
	}

	if ( rule_0.vecEtlVal.size() != rule_1.vecEtlVal.size() )
	{
		return -3;
	}

	return 0;
}

int TaskInfoUtil::CheckPluralEtlRule(std::vector<OneEtlRule>& vec_etlrule)
{
	if ( vec_etlrule.empty() )
	{
		return -1;
	}

	int dim_size = -1;
	int val_size = -1;

	const int V_RULE_SIZE = vec_etlrule.size();
	for ( int i = 0; i < V_RULE_SIZE; ++i )
	{
		OneEtlRule& ref_one = vec_etlrule[i];

		if ( i != 0 )
		{
			if ( ref_one.vecEtlDim.size() != dim_size )
			{
				return -2;
			}

			if ( ref_one.vecEtlVal.size() != val_size )
			{
				return -3;
			}
		}
		else
		{
			dim_size = ref_one.vecEtlDim.size();
			val_size = ref_one.vecEtlVal.size();
		}
	}

	return 0;
}

std::string TaskInfoUtil::GetCompareFields(OneEtlRule& rule_A, OneEtlRule& rule_B, bool inverse /*= false*/)
{
	std::string fields_sql;

	std::string a_tab_prefix;
	std::string b_tab_prefix;
	if ( inverse )		// 反转
	{
		a_tab_prefix = "b.";
		b_tab_prefix = "a.";
	}
	else		// 不反转
	{
		a_tab_prefix = "a.";
		b_tab_prefix = "b.";
	}

	// 采集规则A的维度
	const int DIM_SIZE = rule_A.vecEtlDim.size();
	for ( int i = 0; i < DIM_SIZE; ++i )
	{
		OneEtlDim& dim = rule_A.vecEtlDim[i];

		if ( i != 0 )
		{
			fields_sql += ", " + a_tab_prefix + dim.EtlDimName;
		}
		else
		{
			fields_sql += a_tab_prefix + dim.EtlDimName;
		}
	}

	// 采集规则A的值
	std::string v_sql_A;
	std::string v_sql_B;
	const int VAL_SIZE = rule_A.vecEtlVal.size();
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& val_A = rule_A.vecEtlVal[i];
		OneEtlVal& val_B = rule_B.vecEtlVal[i];

		v_sql_A += ", " + a_tab_prefix + val_A.EtlValName;
		v_sql_B += ", " + b_tab_prefix + val_B.EtlValName;
	}

	fields_sql += v_sql_A + v_sql_B;

	return fields_sql;
}

std::string TaskInfoUtil::GetCompareDims(OneEtlRule& rule_A, OneEtlRule& rule_B)
{
	std::string dims_sql;

	const int DIM_SIZE = rule_A.vecEtlDim.size();
	for ( int i = 0; i < DIM_SIZE; ++i )
	{
		OneEtlDim& dim_a = rule_A.vecEtlDim[i];
		OneEtlDim& dim_b = rule_B.vecEtlDim[i];

		if ( i != 0 )
		{
			dims_sql += " and a." + dim_a.EtlDimName + " = b." + dim_b.EtlDimName;
		}
		else
		{
			dims_sql += "a." + dim_a.EtlDimName + " = b." + dim_b.EtlDimName;
		}
	}

	return dims_sql;
}

std::string TaskInfoUtil::GetCompareEqualVals(OneEtlRule& rule_A, OneEtlRule& rule_B)
{
	std::string vals_sql;

	const int VAL_SIZE = rule_A.vecEtlVal.size();
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& val_a = rule_A.vecEtlVal[i];
		OneEtlVal& val_b = rule_B.vecEtlVal[i];

		if ( i != 0 )
		{
			vals_sql += " and a." + val_a.EtlValName + " = b." + val_b.EtlValName;
		}
		else
		{
			vals_sql += "a." + val_a.EtlValName + " = b." + val_b.EtlValName;
		}
	}

	return vals_sql;
}

std::string TaskInfoUtil::GetCompareUnequalVals(OneEtlRule& rule_A, OneEtlRule& rule_B)
{
	std::string vals_sql;

	const int VAL_SIZE = rule_A.vecEtlVal.size();
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& val_a = rule_A.vecEtlVal[i];
		OneEtlVal& val_b = rule_B.vecEtlVal[i];

		if ( i != 0 )
		{
			vals_sql += " or a." + val_a.EtlValName + " != b." + val_b.EtlValName;
		}
		else
		{
			vals_sql += "a." + val_a.EtlValName + " != b." + val_b.EtlValName;
		}
	}

	return vals_sql;
}

std::string TaskInfoUtil::GetCompareFieldsByCol(OneEtlRule& rule_A, OneEtlRule& rule_B, std::vector<int>& vec_col)
{
	std::string fields_sql;

	// 采集规则A的维度
	const int DIM_SIZE = rule_A.vecEtlDim.size();
	for ( int i = 0; i < DIM_SIZE; ++i )
	{
		OneEtlDim& dim = rule_A.vecEtlDim[i];

		if ( i != 0 )
		{
			fields_sql += ", a." + dim.EtlDimName;
		}
		else
		{
			fields_sql += "a." + dim.EtlDimName;
		}
	}

	// 采集规则A和B的值
	std::string v_sql_A;
	std::string v_sql_B;
	const int COL_SIZE = vec_col.size();
	for ( int i = 0; i < COL_SIZE; ++i )
	{
		int& col_index = vec_col[i];
		OneEtlVal& val_A = rule_A.vecEtlVal[col_index];
		OneEtlVal& val_B = rule_B.vecEtlVal[col_index];

		v_sql_A += ", a." + val_A.EtlValName;
		v_sql_B += ", b." + val_B.EtlValName;
	}

	fields_sql += v_sql_A + v_sql_B;

	return fields_sql;
}

std::string TaskInfoUtil::GetCompareEqualValsByCol(OneEtlRule& rule_A, OneEtlRule& rule_B, std::vector<int>& vec_col)
{
	std::string vals_sql;

	const int COL_SIZE = vec_col.size();
	for ( int i = 0; i < COL_SIZE; ++i )
	{
		int& col_index = vec_col[i];
		OneEtlVal& val_a = rule_A.vecEtlVal[col_index];
		OneEtlVal& val_b = rule_B.vecEtlVal[col_index];

		if ( i != 0 )
		{
			vals_sql += " and a." + val_a.EtlValName + " = b." + val_b.EtlValName;
		}
		else
		{
			vals_sql += "a." + val_a.EtlValName + " = b." + val_b.EtlValName;
		}
	}

	return vals_sql;
}

std::string TaskInfoUtil::GetCompareUnequalValsByCol(OneEtlRule& rule_A, OneEtlRule& rule_B, std::vector<int>& vec_col)
{
	std::string vals_sql;

	const int COL_SIZE = vec_col.size();
	for ( int i = 0; i < COL_SIZE; ++i )
	{
		int& col_index = vec_col[i];
		OneEtlVal& val_a = rule_A.vecEtlVal[col_index];
		OneEtlVal& val_b = rule_B.vecEtlVal[col_index];

		if ( i != 0 )
		{
			vals_sql += " or a." + val_a.EtlValName + " != b." + val_b.EtlValName;
		}
		else
		{
			vals_sql += "a." + val_a.EtlValName + " != b." + val_b.EtlValName;
		}
	}

	return vals_sql;
}

std::string TaskInfoUtil::GetOneRuleFields(OneEtlRule& rule, const std::string& tab_prefix /*= std::string()*/)
{
	std::string fields_sql;

	int v_size = rule.vecEtlDim.size();
	for ( int i = 0; i < v_size; ++i )
	{
		OneEtlDim& dim = rule.vecEtlDim[i];

		if ( i != 0 )
		{
			fields_sql += ", " + tab_prefix + dim.EtlDimName;
		}
		else
		{
			fields_sql += tab_prefix + dim.EtlDimName;
		}
	}

	v_size = rule.vecEtlVal.size();
	for ( int i = 0; i < v_size; ++i )
	{
		OneEtlVal& val = rule.vecEtlVal[i];

		fields_sql += ", " + tab_prefix + val.EtlValName;
	}

	return fields_sql;
}

std::string TaskInfoUtil::GetOneRuleValsNull(OneEtlRule& rule, const std::string& tab_prefix)
{
	std::string val_null_sql;

	const int VAL_SIZE = rule.vecEtlVal.size();
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& ref_val = rule.vecEtlVal[i];

		//val_null_sql += ", NULL";
		if ( i != 0 )
		{
			val_null_sql += " or " + tab_prefix + ref_val.EtlValName + " is null";
		}
		else
		{
			val_null_sql += tab_prefix + ref_val.EtlValName + " is null";
		}
	}

	return val_null_sql;
}

void TaskInfoUtil::GetEtlStatisticsSQLs(std::vector<OneEtlRule>& vec_rules, std::vector<std::string>& vec_hivesql)
{
	std::vector<std::string> v_hive_sql;

	std::string hive_sql;
	const int RULE_SIZE = vec_rules.size();
	for ( int i = 0; i < RULE_SIZE; ++i )
	{
		OneEtlRule& ref_rule = vec_rules[i];

		hive_sql = "select " + GetOneRuleFields(ref_rule);
		hive_sql += " from " + ref_rule.TargetPatch;

		v_hive_sql.push_back(hive_sql);
	}

	v_hive_sql.swap(vec_hivesql);
}

