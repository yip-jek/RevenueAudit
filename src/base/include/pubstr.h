#pragma once

#include <string>
#include <set>
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

	// 计算std::vector<std::string>数量
	static size_t CalcVVVectorStr(std::vector<std::vector<std::vector<std::string> > >& vec3_str);

	// 表达式转换为数值集合
	// 如将 “1-3, 4, 6” 转换为: set[] = 1, 2, 3, 4, 6
	// 返回值: 0-成功, 1-数值重复, -1-转换失败
	static int Express2IntSet(const std::string& src_str, std::set<int>& set_int);

public:
	// 将Vector进行交换push back
	template <typename T>
	static void VVectorSwapPushBack(std::vector<std::vector<T> >& vec2_T, std::vector<T>& vec1_T)
	{
		std::vector<T> vec1_empty;
		vec2_T.push_back(vec1_empty);
		vec1_T.swap(vec2_T[vec2_T.size()-1]);
	}

	// 将嵌套的Vector进行交换push back
	template <typename T>
	static void VVVectorSwapPushBack(std::vector<std::vector<std::vector<T> > >& vec3_T, std::vector<std::vector<T> >& vec2_T)
	{
		std::vector<std::vector<T> > vec2_empty;
		vec3_T.push_back(vec2_empty);
		vec2_T.swap(vec3_T[vec3_T.size()-1]);
	}
};

}	// namespace base

