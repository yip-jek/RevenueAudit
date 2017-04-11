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

// 类型转换
public:
	// (十进制) string -> int
	static bool Str2Int(const std::string& str, int& i);
	// (十进制) string -> unsigned int
	static bool Str2UInt(const std::string& str, unsigned int& u);
	// (十进制) string -> long long
	static bool Str2LLong(const std::string& str, long long& ll);
	// string -> double
	static bool Str2Double(const std::string& str, double& d);
	// string -> long double
	static bool Str2LDouble(const std::string& str, long double& ld);

	// int -> string
	static std::string Int2Str(int i);
	// unsigned int -> string
	static std::string UInt2Str(unsigned int u);
	// long long -> string
	static std::string LLong2Str(long long ll);
	// double -> string
	static std::string Double2Str(double d);
	// long double -> string
	static std::string LDouble2Str(long double ld);

	// double --> [FORMAT] string
	// 保留2位小数
	// 考虑 double 类型的精度，字符串最大长度不超过32
	// double 值过大，会被截断，请谨慎使用！
	static std::string Double2FormatStr(double d);

	// 结合 Str2Double 与 Double2FormatStr
	// 失败则返回空字符串
	static std::string StringDoubleFormat(const std::string& str);

public:
	// 由字符串拆分为字符串数组
	static void Str2StrVector(const std::string& src_str, const std::string& delim, std::vector<std::string>& vec_str);

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

	// 将数组中的值进行格式化
	// 参数 start_pos 和 end_pos 组成前闭后开区间，即[start_pos, end_pos)
	static void FormatValueStrVector(std::vector<std::vector<std::string> >& vec2_str, int start_pos, int end_pos);

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

