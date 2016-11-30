#pragma once

#include "acquire.h"

// 话单稽核-采集模块
class Acquire_HD : public Acquire
{
public:
	Acquire_HD();
	virtual ~Acquire_HD();

	static const char* const S_HD_ETLRULE_TYPE;				// 话单稽核-采集规则类型

protected:
	// 检查采集任务信息
	virtual void CheckTaskInfo() throw(base::Exception);
};

