#pragma once

#include <string>
#include <list>

namespace base
{

class BaseFrameApp;

// 工厂接口
// 创建的对象如果没有销毁，在析构的时候会自动释放
class Factory
{
public:
	Factory();
	virtual ~Factory();

public:
	// 创建
	virtual BaseFrameApp* CreateApp(const std::string& mode, const std::string& var, std::string* pError);

	// 销毁
	virtual void DestroyApp(BaseFrameApp** ppAPP);

protected:
	// 创建对象的接口
	virtual BaseFrameApp* CreateOneApp(const std::string& mode, const std::string& var, std::string* pError) = 0;

protected:
	std::list<BaseFrameApp*> m_listApp;
};

extern Factory* g_pFactory;

}	// namespace base

