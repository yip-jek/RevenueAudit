#pragma once

#include <string>
#include <vector>

class Task;

class TaskFactory
{
public:
	TaskFactory();
	virtual ~TaskFactory();

	static const char* const S_MODE_YC;			// 业财稽核
	static const char* const S_MODE_YD;			// 一点稽核

public:
	// 创建
	Task* Create(const std::string& mode, std::string* pError);

	// 销毁
	bool Destroy(Task*& pTask);

private:
	// 全部销毁
	void DestroyAll();

private:
	std::vector<Task*> m_vecTask;
};

