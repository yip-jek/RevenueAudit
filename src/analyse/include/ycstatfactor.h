#pragma once

#include <map>
#include "ycinfo.h"

// （业财）规则因子类
class YCStatFactor
{
public:
	YCStatFactor();
	virtual ~YCStatFactor();

	//static const char* const S_BASE_PRIORITY;			// 基础优先级
	static const char* const S_TOP_PRIORITY;			// 最高优先级

public:

private:
	std::map<std::string, std::string> m_mDimFactor;		// 维度因子对
};

