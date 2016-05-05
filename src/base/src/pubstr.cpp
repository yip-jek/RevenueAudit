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

}	// namespace base

