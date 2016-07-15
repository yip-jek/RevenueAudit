#pragma once

#include <string>
#include <set>
#include <vector>
#include <boost/lexical_cast.hpp>
#include "exception.h"

namespace base
{

class PubStr
{
public:
	static const int MAX_STR_BUF_SIZE = 4096;

public:
	// 截去前后空白符
	static void Trim(std::string& str);
	static std::string TrimB(const std::string& str);

	// 统一转换为大写
	static void Upper(std::string& str);
	static std::string UpperB(const std::string& str);

	// 截去前后空白符，并且转换为大写
	static void TrimUpper(std::string& str);
	static std::string TrimUpperB(const std::string& str);

	// 设置格式化的字符串
	static void SetFormatString(std::string& str, const char* fmt, ...);

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

	// 表序号转换为表别名
	// 序号从0开始，表别名从a开始
	static std::string TabIndex2TabAlias(int index);

	// 替换字符串数组中的字符串
	static void ReplaceInStrVector2(std::vector<std::vector<std::string> >& vec2_str, const std::string& src_str, const std::string& des_str, bool trim, bool upper);

	// 字符串转换为小数（支持百分数）
	static bool StrTrans2Double(const std::string& str, double& d);

	// 去除数组中每个的字符串结尾的"."和其后的"0"
	static void TrimTail0StrVec2(std::vector<std::vector<std::string> >& vec2_str, int start_pos);

	// 将短精度字符串转换为长精度字符串表示
	static bool DouStr2LongDouStr(std::string& double_str);

	// 将数组中的带 'E' 的精度字符串转换为长精度字符串表示
	static void TransVecDouStrWithE2LongDouStr(std::vector<std::vector<std::string> >& vec2_str, int start_pos);

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

	// 类型转换
	template <typename T1, typename T2>
	static bool T1TransT2(const T1& src, T2& des)
	{
		try
		{
			des = boost::lexical_cast<T2>(src);
			return true;
		}
		catch ( boost::bad_lexical_cast& ex )
		{
			return false;
		}
	}
};

}	// namespace base

