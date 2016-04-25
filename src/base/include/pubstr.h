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
	static void Str2IntVector(const std::string& src_str, const std::string& dim, std::vector<int>& i_vec) throw(Exception);
};

}	// namespace base

