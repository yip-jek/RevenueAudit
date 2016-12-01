#pragma once

#include "factory.h"

namespace base
{

class BaseFrameApp;

}

class Analyse;

class AnaFactory : public base::Factory
{
public:
	AnaFactory();
	virtual ~AnaFactory();

	static const char* const S_MODE_YDJH;				// 一点稽核
	static const char* const S_MODE_YCRA;				// 业财稽核
	static const char* const S_MODE_HDJH;				// 话单稽核

protected:
	// 创建对象
	virtual base::BaseFrameApp* CreateApp(const std::string& mode, std::string* pError);

	// 销毁对象
	virtual void DestroyApp(base::BaseFrameApp** ppApp);
};

