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

std::string TaskInfoUtil::GetCompareFields(OneEtlRule& rule_A, OneEtlRule& rule_B)
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

	// 采集规则A的值
	const int VAL_SIZE = rule_A.vecEtlVal.size();
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& val = rule_A.vecEtlVal[i];

		fields_sql += ", a." + val.EtlValName;
	}

	// 采集规则B的值
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& val = rule_B.vecEtlVal[i];

		fields_sql += ", b." + val.EtlValName;
	}

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

