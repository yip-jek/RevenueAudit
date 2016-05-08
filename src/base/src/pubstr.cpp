#include "pubstr.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "def.h"

namespace base
{

void PubStr::Str2IntVector(const std::string& src_str, const std::string& dim, std::vector<int>& i_vec, bool is_multi_dim) throw(Exception)
{
	std::vector<std::string> vec_str;
	Str2StrVector(src_str, dim, vec_str, is_multi_dim);

	std::vector<int> vec_int;
	const size_t V_SIZE = vec_str.size();

	try
	{
		for ( size_t i = 0; i < V_SIZE; ++i )
		{
			vec_int.push_back(boost::lexical_cast<int>(vec_str[i]));
		}
	}
	catch ( boost::bad_lexical_cast& ex )
	{
		throw Exception(PS_TRANS_FAILED, "字符串(src:%s)转数值数组失败! [BOOST] %s [FILE:%s, LINE:%d]", src_str.c_str(), ex.what(), __FILE__, __LINE__);
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
		boost::trim(vec_str[i]);
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

	try
	{
		const int VEC_SIZE = vec_str.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			Str2StrVector(vec_str[i], "-", vec_section);

			const int V_SIZE = vec_section.size();
			if ( 1 == V_SIZE )		// 单个数字
			{
				const int VAL = boost::lexical_cast<int>(vec_section[0]);

				// 是否重复
				if ( s_int.find(VAL) != s_int.end() )
				{
					return 1;
				}

				s_int.insert(VAL);
			}
			else if ( 2 == V_SIZE )		// 数字区间
			{
				const int LEFT  = boost::lexical_cast<int>(vec_section[0]);	// 左值
				const int RIGHT = boost::lexical_cast<int>(vec_section[1]);	// 右值

				if ( LEFT < RIGHT )		// 区间有效
				{
					for ( int val = LEFT; val <= RIGHT; ++val )
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
	}
	catch ( boost::bad_lexical_cast& ex )	// 转换为数值失败!
	{
		return -1;
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

}	// namespace base

