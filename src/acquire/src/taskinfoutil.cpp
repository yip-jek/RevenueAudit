#include "taskinfoutil.h"
#include "pubstr.h"
#include "acqerror.h"

const char* const TaskInfoUtil::S_SRC_VAL_RECORD       = "<RECORD>";		// 源表的值：记录数
const char* const TaskInfoUtil::S_SRC_VAL_NEGATIVE_SUM = "NEGATIVE_SUM";	// 源表的值：负的和

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

		val_sql += ", sum(" + val.EtlValName + ") as " + val.EtlValName;
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

bool TaskInfoUtil::CheckSpecialVal(const std::string& spec, const std::string& val_src, std::string& val)
{
	std::string val_check = base::PubStr::TrimB(val_src);

	// 是否为格式：特殊标识(值)
	const size_t SPEC_SIZE = spec.size();
	if ( spec == val_check.substr(0, SPEC_SIZE) )
	{
		val_check.erase(0, SPEC_SIZE);
		base::PubStr::Trim(val_check);

		if ( !val_check.empty() && '(' == val_check[0] && ')' == val_check[val_check.size()-1] )
		{
			val_check = base::PubStr::TrimB(val_check.substr(1, val_check.size()-2));

			if ( !val_check.empty() )
			{
				val = val_check;
				return true;
			}
		}
	}

	return false;
}

std::string TaskInfoUtil::AddPrefixVal(const std::string& val_sql, const std::string& prefix)
{
	int start_pos = -1;
	int end_pos   = -1;
	int off_size  = 0;

	std::string prefix_val = val_sql;
	const std::string UP_VAL_SQL = base::PubStr::UpperB(val_sql);

	const int PRE_SIZE = prefix.size();
	const int UVS_SIZE = UP_VAL_SQL.size();
	for ( int i = 0; i < UVS_SIZE; ++i )
	{
		const char CH = UP_VAL_SQL[i];
		if ( start_pos < 0 )
		{
			if ( CH >= 'A' && CH <= 'Z' )	// 字段名以英文字母开头
			{
				start_pos = i;
			}
		}
		else
		{
			if ( CH == '\x20' )
			{
				end_pos = i;
				continue;
			}

			if ( end_pos < 0 )
			{
				if ( CH == '(' )
				{
					start_pos = -1;
				}
				else if ( CH != '_' && (CH < 'A' || CH > 'Z') && (CH < '0' || CH > '9') )
				{
					prefix_val.insert(start_pos+off_size, prefix);

					start_pos = -1;
					off_size += PRE_SIZE;
				}
			}
			else
			{
				if ( CH == '(' )
				{
					start_pos = -1;
					end_pos   = -1;
				}
				else
				{
					prefix_val.insert(start_pos+off_size, prefix);

					start_pos = -1;
					end_pos   = -1;
					off_size += PRE_SIZE;
				}
			}
		}
	}

	if ( start_pos >= 0 )
	{
		prefix_val.insert(start_pos+off_size, prefix);
	}

	return prefix_val;
}

std::string TaskInfoUtil::TransEtlValSrcName(OneEtlVal& val, const std::string& tab_prefix /*= std::string()*/)
{
	std::string val_src = base::PubStr::TrimUpperB(val.EtlValSrcName);
	if ( val_src.empty() )
	{
		return std::string();
	}

	if ( S_SRC_VAL_RECORD == val_src )	// 记录数
	{
		val_src = "count(*)";
	}
	else if ( '[' == val_src[0] && ']' == val_src[val_src.size()-1] )
	{
		val_src = base::PubStr::TrimB(val_src.substr(1, val_src.size()-2));
	}
	else if ( CheckSpecialVal(S_SRC_VAL_NEGATIVE_SUM, val_src, val_src) )
	{
		// 负的和
		val_src = "(-1 * sum(" + val_src + "))";
	}
	else
	{
		//std::string memo = TrimUpperValMemo(val);
		//std::vector<std::string> vec_memo;
		//base::PubStr::Str2StrVector(memo, ":", vec_memo);

		//// 判断是否为运算结果值
		//if ( !vec_memo.empty() )
		//{
		//	std::string& ref_calc = vec_memo[0];

		//	if ( "CALC" == ref_calc )
		//	{
		//		if ( vec_memo.size() != 2 )
		//		{
		//			throw base::Exception(ACQERR_TRANS_VAL_SRC_NAME, "格式不正确！无法识别的备注类型：%s [FILE:%s, LINE:%d]", memo.c_str(), __FILE__, __LINE__);
		//		}

		//		std::string& ref_oper = vec_memo[1];
		//		if ( "-" == ref_oper || "+" == ref_oper )	// 支持加(+)、减(-)运算
		//		{
		//			std::vector<std::string> vec_src;
		//			base::PubStr::Str2StrVector(pval_src, ",", vec_src);

		//			if ( vec_src.size() != 2 )
		//			{
		//				throw base::Exception(ACQERR_TRANS_VAL_SRC_NAME, "源字段个数不匹配！无法识别的值对应源字段名称：%s [FILE:%s, LINE:%d]", val_src.c_str(), __FILE__, __LINE__);
		//			}

		//			val_src = "sum(" + vec_src[0] + ref_oper + vec_src[1] + ")";
		//			return val_src;
		//		}
		//		else
		//		{
		//			throw base::Exception(ACQERR_TRANS_VAL_SRC_NAME, "不支持的运算符！无法识别的备注类型：%s [FILE:%s, LINE:%d]", memo.c_str(), __FILE__, __LINE__);
		//		}
		//	}
		//}

		// 其他情况，直接取sum
		val_src = "sum(" + val_src + ")";
	}

	val_src = AddPrefixVal(val_src, tab_prefix);
	return val_src;
}

std::string TaskInfoUtil::GetEtlValSql(AcqEtlVal& etl_val, const std::string& tab_prefix /*= std::string()*/)
{
	std::string val_sql;

	const int VAL_SIZE = etl_val.vecEtlVal.size();
	for ( int i = 0; i < VAL_SIZE; ++i )
	{
		OneEtlVal& val = etl_val.vecEtlVal[i];

		val_sql += ", " + TransEtlValSrcName(val, tab_prefix) + " as " + val.EtlValName;
	}

	return val_sql;
}

//std::string TaskInfoUtil::TrimUpperDimMemo(OneEtlDim& dim)
//{
//	std::string& memo = dim.EtlDimMemo;
//	base::PubStr::TrimUpper(memo);
//	return memo;
//}

//std::string TaskInfoUtil::TrimUpperValMemo(OneEtlVal& val)
//{
//	std::string& memo = val.EtlValMemo;
//	base::PubStr::TrimUpper(memo);
//	return memo;
//}

//bool TaskInfoUtil::IsOuterJoinOnDim(OneEtlDim& dim)
//{
//	return ("JOIN_ON" == TrimUpperDimMemo(dim));
//}

//bool TaskInfoUtil::IsOuterTabJoinDim(OneEtlDim& dim)
//{
//	return ("JOIN_DIM" == TrimUpperDimMemo(dim));
//}

//bool TaskInfoUtil::IsOuterTabJoinVal(OneEtlVal& val)
//{
//	return ("JOIN_VAL" == TrimUpperValMemo(val));
//}

int TaskInfoUtil::GetNumOfEtlDimJoinOn(AcqEtlDim& etl_dim)
{
	int join_on_count = 0;

	const int DIM_SIZE = etl_dim.vecEtlDim.size();
	for ( int i = 0; i < DIM_SIZE; ++i )
	{
		OneEtlDim& dim = etl_dim.vecEtlDim[i];

		//if ( IsOuterJoinOnDim(dim) )
		if ( OneEtlDim::DMTYPE_JOIN_ON == dim.EtlDimMemo )
		{
			++join_on_count;
		}
	}

	return join_on_count;
}

std::string TaskInfoUtil::GetOuterJoinEtlSQL(AcqEtlDim& etl_dim, AcqEtlVal& etl_val, const std::string& src_tab, const std::string& outer_tab, std::vector<std::string>& vec_join_on, const std::string& cond)
{
	std::string etl_sql = "select ";

	std::string join_on_sql;
	std::string group_by_sql;
	std::string tab_pre;

	bool is_begin = true;
	int join_on_index = 0;
	int vec_size = etl_dim.vecEtlDim.size();
	for ( int i = 0; i < vec_size; ++i )
	{
		OneEtlDim& dim = etl_dim.vecEtlDim[i];

		//if ( IsOuterJoinOnDim(dim) )
		if ( OneEtlDim::DMTYPE_JOIN_ON == dim.EtlDimMemo )
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

		//if ( IsOuterTabJoinDim(dim) )
		tab_pre = (dim.EtlDimMemo != OneEtlDim::DMTYPE_JOIN_DIM) ? "a." : "b.";
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

		//if ( IsOuterTabJoinVal(val) )
		tab_pre = (val.EtlValMemo != OneEtlVal::VMTYPE_JOIN_VAL) ? "a." : "b.";
		etl_sql += ", " + TransEtlValSrcName(val, tab_pre) + " as " + val.EtlValName;
	}

	etl_sql += " from " + src_tab + " a left outer join " + outer_tab;
	etl_sql += " b on (" + join_on_sql + ")" + cond + " group by " + group_by_sql;

	return etl_sql;
}

void TaskInfoUtil::GetTableNames(const std::string& src, std::vector<std::string>& vec_tabname)
{
	const std::string CSTR_SRC = base::PubStr::TrimUpperB(src);

	std::vector<std::string> vec_tmp;
	if ( CSTR_SRC.empty() )		// 空字串
	{
		vec_tmp.swap(vec_tabname);
	}
	else if ( CSTR_SRC.find('\x20') == std::string::npos )	// 中间没有空格，为直接表名
	{
		vec_tmp.push_back(CSTR_SRC);
		vec_tmp.swap(vec_tabname);
	}
	else	// 表名存在于查询SQL语句中
	{
		size_t pos   = 0;
		size_t n_pos = 0;
		const std::string FROM_MARK = " FROM ";
		const size_t      FROM_SIZE = FROM_MARK.size();

		while ( (pos = CSTR_SRC.find(FROM_MARK, pos)) != std::string::npos )
		{
			pos += FROM_SIZE;

			// 后面无内容或者全是空格
			if ( (pos = CSTR_SRC.find_first_not_of('\x20', pos)) == std::string::npos )
			{
				break;
			}

			// 子SQL语句的开始
			if ( '(' == CSTR_SRC[pos] )
			{
				continue;
			}

			if ( (n_pos = CSTR_SRC.find_first_of('\x20', pos)) != std::string::npos )
			{
				if ( CSTR_SRC[n_pos-1] != ')' )
				{
					vec_tmp.push_back(CSTR_SRC.substr(pos, n_pos-pos));
				}
				else
				{
					vec_tmp.push_back(CSTR_SRC.substr(pos, n_pos-pos-1));
				}

				pos = n_pos;
			}
			else	// 末尾
			{
				n_pos = CSTR_SRC.size() - 1;
				if ( CSTR_SRC[n_pos] != ')' )
				{
					vec_tmp.push_back(CSTR_SRC.substr(pos));
				}
				else
				{
					vec_tmp.push_back(CSTR_SRC.substr(pos, n_pos-pos));
				}

				break;
			}
		}

		vec_tmp.swap(vec_tabname);
	}
}

