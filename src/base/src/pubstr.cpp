#include "pubstr.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "def.h"

namespace base
{

void PubStr::Str2IntVector(const std::string& src_str, const std::string& dim, std::vector<int>& i_vec) throw(Exception)
{
	std::vector<std::string> s_vec;
	boost::split(s_vec, src_str, boost::is_any_of(dim));

	std::vector<int> vec_int;
	const size_t V_SIZE = s_vec.size();

	try
	{
		for ( size_t i = 0; i < V_SIZE; ++i )
		{
			std::string& ref_str = s_vec[i];

			boost::trim(ref_str);

			vec_int.push_back(boost::lexical_cast<int>(ref_str));
		}
	}
	catch ( boost::bad_lexical_cast& ex )
	{
		throw Exception(PS_TRANS_FAILED, "字符串(src:%s)转数值数组失败! [BOOST] %s [FILE:%s, LINE:%d]", src_str.c_str(), ex.what(), __FILE__, __LINE__);
	}

	vec_int.swap(i_vec);
}

}	// namespace base

