#include "pubstr.h"
#include <boost/algorithm/string.hpp>
#include "def.h"

namespace base
{

void PubStr::Trim(std::string& str)
{
	boost::trim(str);
}

std::string PubStr::TrimB(const std::string& str)
{
	std::string str_trim = str;
	boost::trim(str_trim);
	return str_trim;
}

void PubStr::Upper(std::string& str)
{
	boost::to_upper(str);
}

std::string PubStr::UpperB(const std::string& str)
{
	std::string str_upper = str;
	boost::to_upper(str_upper);
	return str_upper;
}

void PubStr::TrimUpper(std::string& str)
{
	boost::trim(str);
	boost::to_upper(str);
}

std::string PubStr::TrimUpperB(const std::string& str)
{
	std::string str_tp = str;

	boost::trim(str_tp);
	boost::to_upper(str_tp);

	return str_tp;
}

void PubStr::SetFormatString(std::string& str, const char* fmt, ...)
{
	char buf[MAX_STR_BUF_SIZE] = "";

	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	str = buf;
}

void PubStr::Str2IntVector(const std::string& src_str, const std::string& dim, std::vector<int>& i_vec, bool is_multi_dim) throw(Exception)
{
	std::vector<std::string> vec_str;
	Str2StrVector(src_str, dim, vec_str, is_multi_dim);

	std::vector<int> vec_int;
	const size_t V_SIZE = vec_str.size();

	int int_val = 0;
	for ( size_t i = 0; i < V_SIZE; ++i )
	{
		if ( T1TransT2(vec_str[i], int_val) )
		{
			vec_int.push_back(int_val);
		}
		else
		{
			throw Exception(PS_TRANS_FAILED, "字符串 (src:%s) 转数值数组失败：\"%s\" 无法转换！[FILE:%s, LINE:%d]", src_str.c_str(), vec_str[i].c_str(), __FILE__, __LINE__);
		}
	}

	vec_int.swap(i_vec);
}

void PubStr::Str2StrVector(const std::string& src_str, const std::string& dim, std::vector<std::string>& s_vec, bool is_multi_dim)
{
	std::vector<std::string> vec_str;
	if ( src_str.empty() )
	{
		vec_str.swap(s_vec);
		return;
	}

	if ( is_multi_dim )
	{
		boost::split(vec_str, src_str, boost::is_any_of(dim), boost::algorithm::token_compress_on);
	}
	else
	{
		boost::split(vec_str, src_str, boost::is_any_of(dim), boost::algorithm::token_compress_off);
	}

	const size_t V_SIZE = vec_str.size();
	for( size_t i = 0; i < V_SIZE; ++i )
	{
		Trim(vec_str[i]);
	}

	vec_str.swap(s_vec);
}

size_t PubStr::CalcVVVectorStr(std::vector<std::vector<std::vector<std::string> > >& vec3_str)
{
	size_t calc_size = 0;

	const int VEC3_SIZE = vec3_str.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		calc_size += vec3_str[i].size();
	}

	return calc_size;
}

// 返回值: 0-成功, 1-数值重复, -1-转换失败
int PubStr::Express2IntSet(const std::string& src_str, std::set<int>& set_int)
{
	std::vector<std::string> vec_str;
	Str2StrVector(src_str, ",", vec_str);

	if ( vec_str.empty() )
	{
		return -1;
	}

	std::set<int> s_int;
	std::vector<std::string> vec_section;

	const int VEC_SIZE = vec_str.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		Str2StrVector(vec_str[i], "-", vec_section);

		const int V_SIZE = vec_section.size();
		if ( 1 == V_SIZE )		// 单个数字
		{
			int int_val = 0;
			if ( !T1TransT2(vec_section[0], int_val) )	// 转换失败
			{
				return -1;
			}

			// 是否重复
			if ( s_int.find(int_val) != s_int.end() )
			{
				return 1;
			}

			s_int.insert(int_val);
		}
		else if ( 2 == V_SIZE )		// 数字区间
		{
			int left  = 0;		// 左值
			int right = 0;		// 右值
			if ( !T1TransT2(vec_section[0], left) )		// 转换失败
			{
				return -1;
			}
			if ( !T1TransT2(vec_section[1], right) )	// 转换失败
			{
				return -1;
			}

			if ( left < right )		// 区间有效
			{
				for ( int val = left; val <= right; ++val )
				{
					// 是否重复
					if ( s_int.find(val) != s_int.end() )
					{
						return 1;
					}

					s_int.insert(val);
				}
			}
			else	// 区间无效!
			{
				return -1;
			}
		}
		else	// ERROR!
		{
			return -1;
		}
	}

	s_int.swap(set_int);
	return 0;
}

std::string PubStr::TabIndex2TabAlias(int index)
{
	if ( index < 0 ) 
	{   
		return std::string();
	}   

	int s = index;
	int r = s % 26;

	std::string tab_alias(1, char('a' + r));

	s /= 26;
	while ( s > 0 )
	{
		s -= 1;
		r = s % 26; 

		tab_alias = char('a' + r) + tab_alias;

		s /= 26;
	}

	return tab_alias;
}

void PubStr::ReplaceInStrVector2(std::vector<std::vector<std::string> >& vec2_str, const std::string& src_str, const std::string& des_str, bool trim, bool upper)
{
	const size_t VEC2_SIZE = vec2_str.size();
	for ( size_t i = 0; i < VEC2_SIZE; ++i )
	{
		std::vector<std::string>& ref_vec = vec2_str[i];

		const int VEC1_SIZE = ref_vec.size();
		for ( int j = 0; j < VEC1_SIZE; ++j )
		{
			std::string& ref_str = ref_vec[j];

			if ( trim )		// 去除前后空白符
			{
				Trim(ref_str);
			}
			if ( upper )	// 转换为大写
			{
				Upper(ref_str);
			}

			if ( src_str == ref_str )
			{
				ref_str = des_str;
			}
		}
	}
}

bool PubStr::StrTrans2Double(const std::string& str, double& d)
{
	std::string str_tmp = TrimB(str);
	if ( str_tmp.empty() )
	{
		return false;
	}

	// 是否为百分数
	if ( '%' == str_tmp[str_tmp.size()-1] )
	{
		str_tmp.erase(str_tmp.size()-1);

		if ( T1TransT2(str_tmp, d) )
		{
			d /= 100;
			return true;
		}
		else
		{
			return false;
		}
	}
	else
	{
		return T1TransT2(str_tmp, d);
	}
}

}	// namespace base

