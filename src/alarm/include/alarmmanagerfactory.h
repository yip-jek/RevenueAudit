#pragma once

#include "factory.h"

namespace base
{

class BaseFrameApp;

}

class AlarmManagerFactory : public base::Factory
{
public:
	AlarmManagerFactory();
	virtual ~AlarmManagerFactory();

	static const char* const S_MODE_YDJH;				// 一点稽核

protected:
	// 创建对象
	virtual base::BaseFrameApp* CreateApp(const std::string& mode, std::string* pError);

	// 销毁对象
	virtual void DestroyApp(base::BaseFrameApp** ppApp);
};

