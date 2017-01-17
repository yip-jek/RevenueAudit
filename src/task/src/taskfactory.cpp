#include "taskfactory.h"
#include "pubstr.h"
#include "config.h"
#include "yctask.h"
#include "ydtask.h"

const char* const TaskFactory::S_MODE_YC = "YCJH";			// 业财稽核
const char* const TaskFactory::S_MODE_YD = "YDJH";			// 一点稽核

TaskFactory::TaskFactory(base::Config& cfg)
:m_pCfg(&cfg)
{
}

TaskFactory::~TaskFactory()
{
	DestroyAll();
}

Task* TaskFactory::Create(const std::string& mode, std::string* pError /*= NULL*/)
{
	Task* pTask = NULL;

	const std::string& TASK_MODE = base::PubStr::TrimUpperB(mode);
	if ( TASK_MODE == S_MODE_YC )
	{
		pTask = new YCTask(*m_pCfg);
	}
	else if ( TASK_MODE == S_MODE_YD )
	{
		pTask = new YDTask(*m_pCfg);
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[TASK_FACTORY] Create failed - unspport mode: %s\nOnly support mode: %s %s [FILE:%s, LINE:%d]", mode.c_str(), S_MODE_YC, S_MODE_YD, __FILE__, __LINE__);
		}

		return NULL;
	}

	if ( pTask != NULL )
	{
		m_vecTask.push_back(pTask);
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[TASK_FACTORY] Create failed - operator new failed: 无法申请到内存空间！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
		}
	}

	return pTask;
}

bool TaskFactory::Destroy(Task*& pTask)
{
	if ( pTask != NULL )
	{
		const int VEC_SIZE = m_vecTask.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			if ( pTask == m_vecTask[i] )
			{
				delete pTask;
				pTask = NULL;

				m_vecTask.erase(m_vecTask.begin()+i);
				return true;
			}
		}
	}

	return false;
}

void TaskFactory::DestroyAll()
{
	const int VEC_SIZE = m_vecTask.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		delete m_vecTask[i];
	}

	std::vector<Task*>().swap(m_vecTask);
}

