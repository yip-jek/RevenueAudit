#include "taskfactory.h"

const char* const TaskFactory::S_MODE_YC = "YCJH";			// 业财稽核
const char* const TaskFactory::S_MODE_YD = "YDJH";			// 一点稽核

TaskFactory::TaskFactory()
{
}

TaskFactory::~TaskFactory()
{
	DestroyAll();
}

Task* TaskFactory::Create(const std::string& mode, std::string* pError)
{
}

bool TaskFactory::Destroy(Task*& pTask)
{
	if ( pTask != NULL )
	{
	}
}

void TaskFactory::DestroyAll()
{
}

