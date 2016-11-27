#pragma once

#include <string>
#include <list>

namespace base
{

class BaseFrameApp;

// 工厂接口
class Factory
{
public:
	Factory();
	virtual ~Factory();

	static const char* const S_VAR_DEBUG;				// 测试版本
	static const char* const S_VAR_RELEASE;				// 发布版本

public:
	// 创建
	virtual BaseFrameApp* Create(const std::string& mode, const std::string& var, std::string* pError);

	// 释放资源
	virtual void Release();

protected:
	// 创建对象
	virtual BaseFrameApp* CreateApp(const std::string& mode, std::string* pError) = 0;

	// 销毁对象
	virtual void DestroyApp(BaseFrameApp** ppApp) = 0;

protected:
	std::list<BaseFrameApp*> m_listApp;
};

////////////////////////////////////////////////////////////////////////////////
// 工厂协助类
class FactoryAssist
{
public:
	FactoryAssist(Factory* pFactory): m_pFactory(pFactory)
	{}

	~FactoryAssist()
	{
		if ( m_pFactory != NULL )
		{
			m_pFactory->Release();
		}
	}

private:
	Factory* m_pFactory;
};

}	// namespace base

extern base::Factory* g_pFactory;

