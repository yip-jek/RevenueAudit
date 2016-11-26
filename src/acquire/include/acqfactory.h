#pragma once

#include "factory.h"

// 版本
#define VAR_DEBUG                       ("DEBUG")				// 测试版本
#define VAR_RELEASE                     ("RELEASE")				// 发布版本

// 类型
#define MODE_YDJH                       ("ETL_YDJH")            // 一点稽核
#define MODE_YCRA                       ("ETL_YCRA")            // 业财稽核


namespace base
{

class BaseFrameApp;

}

class Acquire;

class AcqFactory : public base::Factory
{
public:
	AcqFactory();
	virtual ~AcqFactory();

protected:
	// 创建对象
	virtual BaseFrameApp* CreateApp(const std::string& mode, const std::string& var, std::string* pError);

	// 销毁对象
	virtual void DestroyApp(BaseFrameApp** ppApp);

private:
	Acquire* CreateAcq(const std::string& mode, bool is_test, std::string* pError);
};

