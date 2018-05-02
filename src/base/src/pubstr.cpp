#include "pubstr.h"
#include <algorithm>
#include <string.h>

namespace base
{

void PubStr::Trim(std::string& str)
{
	if ( !str.empty() )
	{
		// 去除首部空白符
		const size_t HEAD_POS = strspn(str.c_str(), "\x09\x0A\x0B\x0C\x0D\x20");
		if ( HEAD_POS == str.size() )   // 全为空白符
		{
			str.clear();
			return;
		}

		// 去除尾部空白符
		for ( size_t s = str.size() - 1; s >= HEAD_POS; --s )
		{
			if ( !isspace(str[s]) )
			{
				str.assign(str, HEAD_POS, s+1-HEAD_POS);
				return;
			}
		}
	}
}

std::string PubStr::TrimB(const std::string& str)
{
	std::string t_str = str;
	Trim(t_str);
	return t_str;
}

void PubStr::Upper(std::string& str)
{
	std::transform(str.begin(), str.end(), str.begin(), toupper);
}

std::string PubStr::UpperB(const std::string& str)
{
	std::string u_str = str;
	Upper(u_str);
	return u_str;
}

void PubStr::TrimUpper(std::string& str)
{
	Trim(str);
	Upper(str);
}

std::string PubStr::TrimUpperB(const std::string& str)
{
	std::string tu_str = str;
	TrimUpper(tu_str);
	return tu_str;
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

bool PubStr::Str2Int(const std::string& str, int& i)
{
	std::string t_str = TrimB(str);
	if ( t_str.empty() )
	{
		return false;
	}

	// 去除数字之前的'0'
	size_t pos = t_str.find_first_not_of('0');
	if ( std::string::npos == pos )		// 全为'0'
	{
		t_str = "0";
	}
	else
	{
		t_str.erase(0, pos);
	}

	char* end_ptr = NULL;
	i = strtol(t_str.c_str(), &end_ptr, 0);
	return (*end_ptr == '\0');
}

bool PubStr::Str2UInt(const std::string& str, unsigned int& u)
{
	std::string t_str = TrimB(str);
	if ( t_str.empty() )
	{
		return false;
	}

	// 去除数字之前的'0'
	size_t pos = t_str.find_first_not_of('0');
	if ( std::string::npos == pos )		// 全为'0'
	{
		t_str = "0";
	}
	else
	{
		t_str.erase(0, pos);
	}

	char* end_ptr = NULL;
	u = strtoul(t_str.c_str(), &end_ptr, 0);
	return (*end_ptr == '\0');
}

bool PubStr::Str2LLong(const std::string& str, long long& ll)
{
	std::string t_str = TrimB(str);
	if ( t_str.empty() )
	{
		return false;
	}

	// 去除数字之前的'0'
	size_t pos = t_str.find_first_not_of('0');
	if ( std::string::npos == pos )		// 全为'0'
	{
		t_str = "0";
	}
	else
	{
		t_str.erase(0, pos);
	}

	char* end_ptr = NULL;
	ll = strtoq(t_str.c_str(), &end_ptr, 0);
	return (*end_ptr == '\0');
}

bool PubStr::Str2Double(const std::string& str, double& d)
{
	std::string t_str = TrimB(str);
	if ( t_str.empty() )
	{
		return false;
	}

	char* end_ptr = NULL;
	d = strtod(t_str.c_str(), &end_ptr);
	return (*end_ptr == '\0');
}

bool PubStr::Str2LDouble(const std::string& str, long double& ld)
{
	std::string t_str = TrimB(str);
	if ( t_str.empty() )
	{
		return false;
	}

	char* end_ptr = NULL;
	ld = strtold(t_str.c_str(), &end_ptr);
	return (*end_ptr == '\0');
}

std::string PubStr::Int2Str(int i)
{
	char buf[32] = "";
	sprintf(buf, "%d", i);
	return buf;
}

std::string PubStr::UInt2Str(unsigned int u)
{
	char buf[32] = "";
	sprintf(buf, "%u", u);
	return buf;
}

std::string PubStr::LLong2Str(long long ll)
{
	char buf[32] = "";
	sprintf(buf, "%lld", ll);
	return buf;
}

std::string PubStr::Double2Str(double d)
{
	char buf[64] = "";
	gcvt(d, 63, buf);
	return buf;
}

std::string PubStr::LDouble2Str(long double ld)
{
	char buf[64] = "";
	qgcvt(ld, 63, buf);
	return buf;
}

std::string PubStr::Double2FormatStr(double d)
{
	char buf[32] = "";
	snprintf(buf, sizeof(buf), "%.2lf", d);
	return buf;
}

std::string PubStr::StringDoubleFormat(const std::string& str)
{
	double dou = 0.0;
	if ( Str2Double(str, dou) )
	{
		return Double2FormatStr(dou);
	}

	return std::string();
}

void PubStr::Str2StrVector(const std::string& src_str, const std::string& delim, std::vector<std::string>& vec_str)
{
	std::vector<std::string> v_str;
	if ( src_str.empty() )
	{
		v_str.swap(vec_str);
		return;
	}

	const size_t DLM_SIZE = delim.size();
	if ( DLM_SIZE == 0 )
	{
		v_str.push_back(src_str);
		v_str.swap(vec_str);
		return;
	}

	size_t pos   = 0;
	size_t f_pos = 0;
	while ( (f_pos = src_str.find(delim, pos)) != std::string::npos )
	{
		v_str.push_back(TrimB(src_str.substr(pos, f_pos-pos)));

		pos = f_pos + DLM_SIZE;
	}

	v_str.push_back(TrimB(src_str.substr(pos)));
	v_str.swap(vec_str);
}

size_t PubStr::CalcVVVectorStr(const std::vector<std::vector<std::vector<std::string> > >& vec3_str)
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
			if ( !Str2Int(vec_section[0], int_val) )	// 转换失败
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
			if ( !Str2Int(vec_section[0], left) )		// 转换失败
			{
				return -1;
			}
			if ( !Str2Int(vec_section[1], right) )	// 转换失败
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

		if ( Str2Double(str_tmp, d) )
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
		return Str2Double(str_tmp, d);
	}
}

void PubStr::FormatValueStrVector(std::vector<std::vector<std::string> >& vec2_str, int start_pos, int end_pos)
{
	const int   BEGIN_POS = (start_pos < 0 ? 0 : start_pos);
	std::string str_val;

	const size_t VEC2_SIZE = vec2_str.size();
	for ( size_t i = 0; i < VEC2_SIZE; ++i )
	{
		std::vector<std::string>& ref_vec = vec2_str[i];

		const int LAST_POS = (end_pos < (int)ref_vec.size() ? end_pos : ref_vec.size());
		for ( int j = BEGIN_POS; j < LAST_POS; ++j )
		{
			std::string& ref_str = ref_vec[j];

			// 转换失败，则保持原值不变！
			str_val = StringDoubleFormat(ref_str);
			if ( !str_val.empty() )
			{
				ref_str = str_val;
			}
		}
	}
}

}	// namespace base

