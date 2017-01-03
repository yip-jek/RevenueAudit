#pragma once

#include "analyse.h"

// 一点稽核-分析模块
class Analyse_YD : public Analyse
{
public:
	Analyse_YD();
	virtual ~Analyse_YD();

public:
	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();
};

