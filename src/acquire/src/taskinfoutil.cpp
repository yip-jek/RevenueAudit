#include "taskinfoutil.h"
#include <boost/algorithm/string.hpp>

std::string TaskInfoUtil::GetTargetDimSql(AcqEtlDim& target_dim)
{
	std::string dim_sql;

	const int DIM_SIZE = target_dim.vecEtlDim.size();
	for ( int i = 0; i < DIM_SIZE; ++i )
	{
		OneEtlDim& dim = target_dim.vecEtlDim[i];

		// 忽略无效维度
		if ( dim.EtlDimSeq < 0 )
		{
			continue;
		}

		if ( i != 0 )
		{
			dim_sql += ", " + dim.EtlDimName;
		}
		else
		{
			dim_sql += dim.EtlDimName;
		}
	}

	return dim_sql;
}

std::string TaskInfoUtil::GetTargetValSql(AcqEtlVal& target_val)
{
	std::string val_sql;

	const int VAL_SIZE = target_val.vecEtlVal.size();
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& val = target_val.vecEtlVal[i];

		// 忽略无效值
		if ( val.EtlValSeq < 0 )
		{
			continue;
		}

		val_sql += ", sum(" + val.EtlValName + ")";
	}

	return val_sql;
}

std::string TaskInfoUtil::GetEtlDimSql(AcqEtlDim& etl_dim, bool set_as, const std::string& tab_prefix /*= std::string()*/)
{
	std::string dim_sql;

	const int DIM_SIZE = etl_dim.vecEtlDim.size();
	if ( set_as )		// 指定别名
	{
		for ( int i = 0; i < DIM_SIZE; ++i )
		{
			OneEtlDim& dim = etl_dim.vecEtlDim[i];

			if ( i != 0 )
			{
				dim_sql += ", " + tab_prefix + dim.EtlDimSrcName + " as " + dim.EtlDimName;
			}
			else
			{
				dim_sql += tab_prefix + dim.EtlDimSrcName + " as " + dim.EtlDimName;
			}
		}
	}
	else	// 不指定别名
	{
		for ( int i = 0; i < DIM_SIZE; ++i )
		{
			OneEtlDim& dim = etl_dim.vecEtlDim[i];

			if ( i != 0 )
			{
				dim_sql += ", " + tab_prefix + dim.EtlDimSrcName;
			}
			else
			{
				dim_sql += tab_prefix + dim.EtlDimSrcName;
			}
		}
	}

	return dim_sql;
}

std::string TaskInfoUtil::TransEtlValSrcName(const std::string& val_srcname, const std::string& tab_prefix /*= std::string()*/)
{
	std::string val = val_srcname;

	boost::trim(val);
	boost::to_upper(val);

	if ( "<RECORD>" == val )	// 记录数
	{
		val = "count(*)";
	}
	else
	{
		val = "sum(" + tab_prefix + val + ")";
	}

	return val;
}

std::string TaskInfoUtil::GetEtlValSql(AcqEtlVal& etl_val, const std::string& tab_prefix /*= std::string()*/)
{
	std::string val_sql;

	const int VAL_SIZE = etl_val.vecEtlVal.size();
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& val = etl_val.vecEtlVal[i];

		val_sql += ", " + TransEtlValSrcName(val.EtlValSrcName, tab_prefix) + " as " + val.EtlValName;
	}

	return val_sql;
}

bool TaskInfoUtil::IsOuterJoinOnDim(OneEtlDim& dim)
{
	std::string& memo = dim.EtlDimMemo;

	boost::trim(memo);
	boost::to_upper(memo);

	return ("JOIN_ON" == memo);
}

bool TaskInfoUtil::IsOuterTabJoinDim(OneEtlDim& dim)
{
	std::string& memo = dim.EtlDimMemo;

	boost::trim(memo);
	boost::to_upper(memo);

	return ("JOIN_DIM" == memo);
}

bool TaskInfoUtil::IsOuterTabJoinVal(OneEtlVal& val)
{
	std::string& memo = val.EtlValMemo;

	boost::trim(memo);
	boost::to_upper(memo);

	return ("JOIN_VAL" == memo);
}

int TaskInfoUtil::GetNumOfEtlDimJoinOn(AcqEtlDim& etl_dim)
{
	int join_on_count = 0;

	const int DIM_SIZE = etl_dim.vecEtlDim.size();
	for ( int i = 0; i < DIM_SIZE; ++i )
	{
		OneEtlDim& dim = etl_dim.vecEtlDim[i];

		if ( IsOuterJoinOnDim(dim) )
		{
			++join_on_count;
		}
	}

	return join_on_count;
}

std::string TaskInfoUtil::GetOuterJoinEtlSQL(AcqEtlDim& etl_dim, AcqEtlVal& etl_val, const std::string& src_tab, const std::string& outer_tab, std::vector<std::string>& vec_join_on)
{
	std::string etl_sql = " select ";

	std::string join_on_sql;
	std::string group_by_sql;
	std::string tab_pre;

	int join_on_index = 0;
	int vec_size = etl_dim.vecEtlDim.size();
	for ( int i = 0; i < vec_size; ++i )
	{
		OneEtlDim& dim = etl_dim.vecEtlDim[i];

		if ( IsOuterJoinOnDim(dim) )
		{
			if ( join_on_index != 0 )
			{
				join_on_sql += " and a." + dim.EtlDimSrcName + " = b." + vec_join_on[join_on_index];
			}
			else
			{
				join_on_sql += "a." + dim.EtlDimSrcName + " = b." + vec_join_on[join_on_index];
			}

			++join_on_index;
		}

		if ( dim.EtlDimSeq < 0 )
		{
			continue;
		}

		if ( IsOuterTabJoinDim(dim) )
		{
			tab_pre = "b.";
		}
		else
		{
			tab_pre = "a.";
		}

		if ( i != 0 )
		{
			etl_sql += ", " + tab_pre + dim.EtlDimSrcName + " as " + dim.EtlDimName;
			group_by_sql += ", " + tab_pre + dim.EtlDimSrcName;
		}
		else
		{
			etl_sql += tab_pre + dim.EtlDimSrcName + " as " + dim.EtlDimName;
			group_by_sql += tab_pre + dim.EtlDimSrcName;
		}
	}

	vec_size = etl_val.vecEtlVal.size();
	for ( int i = 0; i < vec_size; ++i )
	{
		OneEtlVal& val = etl_val.vecEtlVal[i];

		if ( val.EtlValSeq < 0 )
		{
			continue;
		}

		if ( IsOuterTabJoinVal(val) )
		{
			tab_pre = "b.";
		}
		else
		{
			tab_pre = "a.";
		}

		etl_sql += ", " + tab_pre + val.EtlValSrcName + " as " + val.EtlValName;
	}

	etl_sql += " from " + src_tab + " a left outer join " + outer_tab;
	etl_sql += " b on (" + join_on_sql + ") group by " + group_by_sql;

	return etl_sql;
}

