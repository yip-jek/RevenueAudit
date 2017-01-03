#pragma once

#include "acquire.h"

// 一点稽核-采集模块
class Acquire_YD : public Acquire
{
public:
	Acquire_YD();
	virtual ~Acquire_YD();

public:
	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();
};

