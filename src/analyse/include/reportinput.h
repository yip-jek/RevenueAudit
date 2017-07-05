#pragma once

#include "exception.h"

class ReportInput
{
public:
	ReportInput() {}
	virtual ~ReportInput() {}

public:
	// 获取数据组数目
	virtual unsigned int GetDataGroupSize() = 0;

	// 获取数据大小
	virtual unsigned int GetDataSize(unsigned int group_index) throw(base::Exception) = 0;

	// 获取特征值（key值）
	virtual std::string GetKey(unsigned int group_index, unsigned int date_index) throw(base::Exception) = 0;

};

