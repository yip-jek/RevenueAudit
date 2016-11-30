#include "taskinfoutil.h"
#include "pubstr.h"

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

	size_t dim_size = 0;
	size_t val_size = 0;

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

std::string TaskInfoUtil::GetOneDim(std::vector<OneEtlDim>& vec_dim, const std::string& prefix)
{
	std::string dim_sql;
	const int VEC_DIM_SIZE = vec_dim.size();
	for ( int i = 0; i < VEC_DIM_SIZE; ++i )
	{
		OneEtlDim& ref_dim = vec_dim[i];

		if ( dim_sql.empty() )
		{
			dim_sql = prefix + ref_dim.EtlDimName;
		}
		else
		{
			dim_sql += ", " + prefix + ref_dim.EtlDimName;
		}
	}

	return dim_sql;
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

std::string TaskInfoUtil::GetCompareFieldsByCol(OneEtlRule& rule_A, OneEtlRule& rule_B, std::vector<int>& vec_col, bool inverse /*= false*/)
{
	std::string fields_sql;

	std::string tab_prefix_1;
	std::string tab_prefix_2;
	if ( inverse )		// 反转
	{
		tab_prefix_1 = "b.";
		tab_prefix_2 = "a.";
	}
	else		// 不反转
	{
		tab_prefix_1 = "a.";
		tab_prefix_2 = "b.";
	}

	// 采集规则A的维度
	fields_sql += GetOneDim(rule_A.vecEtlDim, tab_prefix_1);

	// 采集规则A和B的值，以及它们的差值
	std::string v_sql_A;
	std::string v_sql_B;
	std::string v_sql_diff;

	if ( vec_col.empty() )		// 没有指定列集，则默认所有列
	{
		const int VAL_SIZE = rule_A.vecEtlVal.size();
		for ( int i = 0; i < VAL_SIZE; ++i )
		{
			OneEtlVal& val_A = rule_A.vecEtlVal[i];
			OneEtlVal& val_B = rule_B.vecEtlVal[i];

			v_sql_A += ", " + tab_prefix_1 + val_A.EtlValName;
			v_sql_B += ", " + tab_prefix_2 + val_B.EtlValName;

			v_sql_diff += ", abs(" + tab_prefix_1 + val_A.EtlValName + "-" + tab_prefix_2 + val_B.EtlValName + ")";
		}
	}
	else	// 指定列集
	{
		const int COL_SIZE = vec_col.size();
		for ( int i = 0; i < COL_SIZE; ++i )
		{
			int& col_index = vec_col[i];
			OneEtlVal& val_A = rule_A.vecEtlVal[col_index];
			OneEtlVal& val_B = rule_B.vecEtlVal[col_index];

			v_sql_A += ", " + tab_prefix_1 + val_A.EtlValName;
			v_sql_B += ", " + tab_prefix_2 + val_B.EtlValName;

			v_sql_diff += ", abs(" + tab_prefix_1 + val_A.EtlValName + "-" + tab_prefix_2 + val_B.EtlValName + ")";
		}
	}

	fields_sql += v_sql_A + v_sql_B + v_sql_diff;

	return fields_sql;
}

std::string TaskInfoUtil::GetBothSingleDims(OneEtlRule& rule_A, OneEtlRule& rule_B)
{
	std::string both_single_dims_sql;

	// 采集规则 A 的单独显示维度列集
	std::string single_dim_sql = GetOneDim(rule_A.vecEtlSingleDim, "a.");
	if ( !single_dim_sql.empty() )
	{
		both_single_dims_sql += (", " + single_dim_sql);
	}

	// 采集规则 B 的单独显示维度列集
	single_dim_sql = GetOneDim(rule_B.vecEtlSingleDim, "b.");
	if ( !single_dim_sql.empty() )
	{
		both_single_dims_sql += (", " + single_dim_sql);
	}

	return both_single_dims_sql;
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

void TaskInfoUtil::GetOneRuleFields(std::string& dim_sql, std::string& val_sql, OneEtlRule& rule, bool set_as, const std::string& tab_prefix /*= std::string()*/)
{
	// 维度 SQL
	std::string sql_dim = GetOneDim(rule.vecEtlDim, tab_prefix);

	// 值 SQL
	std::string sql_val;
	const int VEC_VAL_SIZE = rule.vecEtlVal.size();
	if ( set_as )	// 带别名
	{
		for ( int i = 0; i < VEC_VAL_SIZE; ++i )
		{
			OneEtlVal& val = rule.vecEtlVal[i];

			sql_val += ", sum(" + tab_prefix + val.EtlValName + ") as " + val.EtlValName;
		}
	}
	else	// 不带别名
	{
		for ( int i = 0; i < VEC_VAL_SIZE; ++i )
		{
			OneEtlVal& val = rule.vecEtlVal[i];

			sql_val += ", sum(" + tab_prefix + val.EtlValName + ")";
		}
	}

	dim_sql = sql_dim;
	val_sql = sql_val;
}

std::string TaskInfoUtil::GetOneEtlRuleDetailSQL(OneEtlRule& one_rule)
{
	// 生成单独显示的维度列集 SQL
	std::string single_dim_sql;
	std::string one_dim_sql = GetOneDim(one_rule.vecEtlSingleDim, "");
	if ( !one_dim_sql.empty() )
	{
		single_dim_sql = ", " + one_dim_sql;
	}

	std::string dim_sql;
	std::string val_sql;
	GetOneRuleFields(dim_sql, val_sql, one_rule, false);

	std::string detail_sql = "select " + dim_sql + val_sql + single_dim_sql;
	detail_sql += " from " + one_rule.TargetPatch + " group by " + dim_sql + single_dim_sql;
	return detail_sql;
}

std::string TaskInfoUtil::GetOneRuleValsNull(OneEtlRule& rule, const std::string& tab_prefix)
{
	std::string val_null_sql;

	const int VAL_SIZE = rule.vecEtlVal.size();
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& ref_val = rule.vecEtlVal[i];

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

void TaskInfoUtil::GetEtlStatisticsSQLs(std::vector<OneEtlRule>& vec_rules, std::vector<std::string>& vec_hivesql, bool union_all)
{
	std::vector<std::string> v_hive_sql;
	if ( vec_rules.empty() )
	{
		v_hive_sql.swap(vec_hivesql);
		return;
	}

	if ( union_all )	// 通过union all联合多个语句
	{
		std::string gp_dim_sql;
		std::string val_sql;

		// HIVE SQL HEAD
		GetOneRuleFields(gp_dim_sql, val_sql, vec_rules[0], false);
		std::string hive_sql = "select " + gp_dim_sql + val_sql + " from (";

		std::string dim_sql;
		std::string tab_alias;
		std::string tab_pre;

		// HIVE SQL BODY
		const int RULE_SIZE = vec_rules.size();
		for ( int i = 0; i < RULE_SIZE; ++i )
		{
			OneEtlRule& ref_rule = vec_rules[i];

			if ( i != 0 )
			{
				hive_sql += " union all ";
			}

			tab_alias = base::PubStr::TabIndex2TabAlias(i);
			tab_pre   = tab_alias + ".";

			GetOneRuleFields(dim_sql, val_sql, ref_rule, true, tab_pre);

			hive_sql += "select " + dim_sql + val_sql + " from " + ref_rule.TargetPatch + " " + tab_alias;
			hive_sql += " group by " + dim_sql;
		}

		// HIVE SQL TAIL
		hive_sql += ") TMP group by " + gp_dim_sql;

		v_hive_sql.push_back(hive_sql);
	}
	else	// 每个语句独立
	{
		std::string hive_sql;
		std::string dim_sql;
		std::string val_sql;

		const int RULE_SIZE = vec_rules.size();
		for ( int i = 0; i < RULE_SIZE; ++i )
		{
			OneEtlRule& ref_rule = vec_rules[i];

			GetOneRuleFields(dim_sql, val_sql, ref_rule, false);

			hive_sql = "select " + dim_sql + val_sql + " from ";
			hive_sql += ref_rule.TargetPatch + " group by " + dim_sql;

			v_hive_sql.push_back(hive_sql);
		}
	}

	v_hive_sql.swap(vec_hivesql);
}

void TaskInfoUtil::GetEtlStatisticsSQLsBySet(std::vector<OneEtlRule>& vec_rules, std::set<int>& set_int, std::vector<std::string>& vec_hivesql, bool union_all)
{
	std::vector<std::string> v_hive_sql;
	if ( vec_rules.empty() )
	{
		v_hive_sql.swap(vec_hivesql);
		return;
	}

	if ( union_all )	// 通过union all联合多个语句
	{
		std::string gp_dim_sql;
		std::string val_sql;

		// HIVE SQL HEAD
		GetOneRuleFields(gp_dim_sql, val_sql, vec_rules[0], false);
		std::string hive_sql = "select " + gp_dim_sql + val_sql + " from (";

		std::string dim_sql;
		std::string tab_alias;
		std::string tab_pre;

		// HIVE SQL BODY
		for ( std::set<int>::iterator it = set_int.begin(); it != set_int.end(); ++it )
		{
			// set<int>是从1开始的，用作索引需要减一
			int index = (*it) - 1;
			OneEtlRule& ref_rule = vec_rules[index];

			if ( it != set_int.begin() )
			{
				hive_sql += " union all ";
			}

			tab_alias = base::PubStr::TabIndex2TabAlias(index);
			tab_pre   = tab_alias + ".";

			GetOneRuleFields(dim_sql, val_sql, ref_rule, true, tab_pre);

			hive_sql += "select " + dim_sql + val_sql + " from " + ref_rule.TargetPatch + " " + tab_alias;
			hive_sql += " group by " + dim_sql;
		}

		// HIVE SQL TAIL
		hive_sql += ") TMP group by " + gp_dim_sql;

		v_hive_sql.push_back(hive_sql);
	}
	else	// 每个语句独立
	{
		std::string hive_sql;
		std::string dim_sql;
		std::string val_sql;

		for ( std::set<int>::iterator it = set_int.begin(); it != set_int.end(); ++it )
		{
			// set<int>是从1开始的，用作索引需要减一
			int index = (*it) - 1;
			OneEtlRule& ref_rule = vec_rules[index];

			GetOneRuleFields(dim_sql, val_sql, ref_rule, false);

			hive_sql = "select " + dim_sql + val_sql + " from ";
			hive_sql += ref_rule.TargetPatch + " group by " + dim_sql;

			v_hive_sql.push_back(hive_sql);
		}
	}

	v_hive_sql.swap(vec_hivesql);
}

bool TaskInfoUtil::KpiColumnPushBackAnaFields(std::vector<AnaField>& vec_anafields, std::vector<KpiColumn>& vec_kpicol, std::string& err_msg)
{
	AnaField ana_field;

	const int KPI_COL_SIZE = vec_kpicol.size();
	for ( int i = 0; i < KPI_COL_SIZE; ++i )
	{
		KpiColumn& ref_kpicol = vec_kpicol[i];

		if ( ref_kpicol.DBName.empty() )
		{
			base::PubStr::SetFormatString(err_msg, "The DBName of KpiColumn is a blank! Invalid! (KPI_ID=%s, COL_TYPE=%s, COL_SEQ=%d)", ref_kpicol.KpiID.c_str(), (KpiColumn::CTYPE_DIM==ref_kpicol.ColType?"DIM":"VAL"), ref_kpicol.ColSeq);
			return false;
		}

		if ( ref_kpicol.ColSeq < 0 )
		{
			base::PubStr::SetFormatString(err_msg, "The ColSeq of KpiColumn is invalid: %d (KPI_ID=%s, COL_TYPE=%s, DB_NAME=%s)", ref_kpicol.ColSeq, ref_kpicol.KpiID.c_str(), (KpiColumn::CTYPE_DIM==ref_kpicol.ColType?"DIM":"VAL"), ref_kpicol.DBName.c_str());
			return false;
		}

		ana_field.field_name = ref_kpicol.DBName;
		ana_field.CN_name    = ref_kpicol.CNName;
		vec_anafields.push_back(ana_field);
	}

	return true;
}

//std::string TaskInfoUtil::GetStraightAnaCondition(AnalyseRule::AnalyseConditionType cond_type, const std::string& condition, bool add)
//{
//	if ( AnalyseRule::ACTYPE_STRAIGHT == cond_type )	// 直接条件
//	{
//		std::string ana_condition = base::PubStr::TrimB(condition);
//
//		std::string head_cond = base::PubStr::UpperB(ana_condition.substr(0, 5));
//
//		if ( add )		// 作为后续的条件
//		{
//			// 去除句首的 "where"
//			if ( "WHERE" == head_cond )
//			{
//				ana_condition.erase(0, 5);
//				base::PubStr::Trim(ana_condition);
//			}
//
//			ana_condition = " and (" + ana_condition + ")";
//		}
//		else	// 作为新的条件
//		{
//			// 加上 "where"
//			if ( head_cond != "WHERE" )		// 不带 where
//			{
//				ana_condition = " where " + ana_condition;
//			}
//			else	// 带 where
//			{
//				ana_condition = " " + ana_condition;
//			}
//		}
//
//		return ana_condition;
//	}
//	else		// 其他条件
//	{
//		return std::string();
//	}
//}
//
//void TaskInfoUtil::AddConditionSql(std::string& src_sql, const std::string& cond_sql)
//{
//	if ( !cond_sql.empty() )
//	{
//		std::string sql = base::PubStr::UpperB(src_sql);
//
//		// 是否有关键词 "group"
//		const size_t POS = sql.find(" GROUP ");
//		if ( POS != std::string::npos )		// 带关键词
//		{
//			src_sql.insert(POS, cond_sql);
//		}
//		else		// 不带关键词
//		{
//			src_sql += cond_sql;
//		}
//	}
//}

