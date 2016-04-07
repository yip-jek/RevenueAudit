#pragma once

#include <string>
#include <list>

class PubStr
{
public:
	static void Trim(std::string& str);
	static void Upper(std::string& str);
	static std::string LLong2Str(long long ll);
	static int Str2Int(const std::string& str);
	static long long Str2LLong(const std::string& str);
	static float Str2Float(const std::string& str);
	static int SplitStr(const std::string& str, const std::string& delim, std::list<std::string>& list, bool b_trim);
};

