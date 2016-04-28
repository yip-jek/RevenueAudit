#pragma once

#include <string>
#include <vector>
#include "exception.h"

namespace base
{

class PubStr
{
public:
	// 由字符串拆分为数值数组
	// is_multi_dim为true时，表示多个连接的分隔符当作单个分隔符来处理
	static void Str2IntVector(const std::string& src_str, const std::string& dim, std::vector<int>& i_vec, bool is_multi_dim = false) throw(Exception);

	// 由字符串拆分为字符串数组
	// is_multi_dim为true时，表示多个连接的分隔符当作单个分隔符来处理
	static void Str2StrVector(const std::string& src_str, const std::string& dim, std::vector<std::string>& s_vec, bool is_multi_dim = false);
};

}	// namespace base

