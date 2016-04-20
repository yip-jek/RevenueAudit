#include "pubstr.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

namespace base
{

void PubStr::Trim(std::string& str)
{
	if ( !str.empty() )
	{
		std::string white_spaces(" \t\f\v\n\r");
		std::size_t first_pos = str.find_first_not_of(white_spaces);
		if ( std::string::npos == first_pos )
		{
			str.clear();
		}
		else
		{
			std::size_t last_pos = str.find_last_not_of(white_spaces);
			str = str.substr(first_pos, last_pos-first_pos+1);
		}
	}
}

void PubStr::Upper(std::string& str)
{
	if ( !str.empty() )
	{
		const int STR_SIZE = str.size();
		for ( int i = 0; i < STR_SIZE; ++i )
		{
			str[i] = toupper(str[i]);
		}
	}
}

std::string PubStr::LLong2Str(long long ll)
{
	char buf[64] = "";
	snprintf(buf, sizeof(buf), "%lld", ll);
	return buf;
}

int PubStr::Str2Int(const std::string& str)
{
	return atoi(str.c_str());
}

long long PubStr::Str2LLong(const std::string& str)
{
	return atoll(str.c_str());
}

float PubStr::Str2Float(const std::string& str)
{
	return atof(str.c_str());
}

int PubStr::SplitStr(const std::string& str, const std::string& delim, std::list<std::string>& list, bool b_trim)
{
	if ( str.empty() || delim.empty() )
	{
		return 0;
	}

	const size_t SEP_SIZE = delim.size();
	size_t pos = 0;
	size_t f_pos = 0;

	std::string sub_str;
	while ( (f_pos = str.find(delim, pos)) != std::string::npos )
	{
		sub_str = str.substr(pos, f_pos-pos);
		if ( b_trim )
		{
			Trim(sub_str);
		}

		list.push_back(sub_str);

		pos = f_pos + SEP_SIZE;
	}

	sub_str = str.substr(pos);
	if ( b_trim )
	{
		Trim(sub_str);
	}

	list.push_back(sub_str);

	return list.size();
}

}	// namespace base

