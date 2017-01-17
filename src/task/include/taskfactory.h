#pragma once

#include <string>
#include <vector>

namespace base
{
class Config;
}

class Task;

class TaskFactory
{
public:
	TaskFactory(base::Config& cfg);
	virtual ~TaskFactory();

	static const char* const S_MODE_YC;			// 业财稽核
	static const char* const S_MODE_YD;			// 一点稽核

public:
	// 创建
	Task* Create(const std::string& mode, std::string* pError = NULL);

	// 销毁
	bool Destroy(Task*& pTask);

private:
	// 全部销毁
	void DestroyAll();

private:
	base::Config*      m_pCfg;					// 配置文件
	std::vector<Task*> m_vecTask;
};

