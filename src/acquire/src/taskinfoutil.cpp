#include "taskinfoutil.h"
#include <boost/algorithm/string.hpp>

std::string TaskInfoUtil::GetTargetDimSql(AcqEtlDim& target_dim)
{
	std::string dim_sql;

	bool is_begin = true;

	const int DIM_SIZE = target_dim.vecEtlDim.size();
	for ( int i = 0; i < DIM_SIZE; ++i )
	{
		OneEtlDim& dim = target_dim.vecEtlDim[i];

		// 忽略无效维度
		if ( dim.EtlDimSeq < 0 )
		{
			continue;
		}

		if ( is_begin )
		{
			dim_sql += dim.EtlDimName;
			is_begin = false;
		}
		else
		{
			dim_sql += ", " + dim.EtlDimName;
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

std::string TaskInfoUtil::TransEtlValSrcName(OneEtlVal& val, const std::string& tab_prefix /*= std::string()*/) throw(base::Exception)
{
	std::string val_src = val.EtlValSrcName;

	boost::trim(val_src);
	boost::to_upper(val_src);

	if ( "<RECORD>" == val_src )	// 记录数
	{
		val_src = "count(*)";
	}
	else
	{
		std::string memo = TrimUpperValMemo(val);
		std::vector<std::string> vec_memo;
		base::PubStr::Str2StrVector(memo, ":", vec_memo);

		// 判断是否为运算结果值
		if ( !vec_memo.empty() )
		{
			std::string& ref_calc = vec_memo[0];

			if ( "CALC" == ref_calc )
			{
				if ( vec_memo.size() != 2 )
				{
					throw base::Exception(TERR_TRANS_VAL_SRC_NAME, "格式不正确！无法识别的备注类型：%s [FILE:%s, LINE:%d]", memo.c_str(), __FILE__, __LINE__);
				}

				std::string& ref_oper = vec_memo[1];
				if ( "-" == ref_oper || "+" == ref_oper )	// 支持加(+)、减(-)运算
				{
					std::vector<std::string> vec_src;
					base::PubStr::Str2StrVector(val_src, ",", vec_src);

					if ( vec_src.size() != 2 )
					{
						throw base::Exception(TERR_TRANS_VAL_SRC_NAME, "源字段个数不匹配！无法识别的值对应源字段名称：%s [FILE:%s, LINE:%d]", val_src.c_str(), __FILE__, __LINE__);
					}

					val_src = "sum(" + tab_prefix + vec_src[0] + ref_oper + tab_prefix + vec_src[1] + ")";
					return val_src;
				}
				else
				{
					throw base::Exception(TERR_TRANS_VAL_SRC_NAME, "不支持的运算符！无法识别的备注类型：%s [FILE:%s, LINE:%d]", memo.c_str(), __FILE__, __LINE__);
				}
			}
		}

		// 其他情况，直接取sum
		val_src = "sum(" + tab_prefix + val_src + ")";
	}

	return val_src;
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

std::string TaskInfoUtil::TrimUpperDimMemo(OneEtlDim& dim)
{
	std::string& memo = dim.EtlDimMemo;

	boost::trim(memo);
	boost::to_upper(memo);

	return memo;
}

std::string TaskInfoUtil::TrimUpperValMemo(OneEtlVal& val)
{
	std::string& memo = val.EtlValMemo;

	boost::trim(memo);
	boost::to_upper(memo);

	return memo;
}

bool TaskInfoUtil::IsOuterJoinOnDim(OneEtlDim& dim)
{
	return ("JOIN_ON" == TrimUpperDimMemo(dim));
}

bool TaskInfoUtil::IsOuterTabJoinDim(OneEtlDim& dim)
{
	return ("JOIN_DIM" == TrimUpperDimMemo(dim));
}

bool TaskInfoUtil::IsOuterTabJoinVal(OneEtlVal& val)
{
	return ("JOIN_VAL" == TrimUpperValMemo(val));
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

	bool is_begin = true;
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

		if ( is_begin )
		{
			etl_sql += tab_pre + dim.EtlDimSrcName + " as " + dim.EtlDimName;
			group_by_sql += tab_pre + dim.EtlDimSrcName;

			is_begin = false;
		}
		else
		{
			etl_sql += ", " + tab_pre + dim.EtlDimSrcName + " as " + dim.EtlDimName;
			group_by_sql += ", " + tab_pre + dim.EtlDimSrcName;
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

		etl_sql += ", " + TransEtlValSrcName(val.EtlValSrcName, tab_pre) + " as " + val.EtlValName;
	}

	etl_sql += " from " + src_tab + " a left outer join " + outer_tab;
	etl_sql += " b on (" + join_on_sql + ") group by " + group_by_sql;

	return etl_sql;
}

