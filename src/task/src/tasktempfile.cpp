#include "tasktempfile.h"
#include "basefile.h"
#include "exception.h"
#include "log.h"
#include "simpletime.h"

const char* const TaskTempFile::S_PREFIX_TEMP_FILE = "TASK_TEMP_";		// 临时文件名的前缀
const char* const TaskTempFile::S_STATE_SEGMENT    = "STATE";			// 配置：状态段名称
const char* const TaskTempFile::S_PAUSE_ITEM       = "PAUSE";			// 配置：暂停项名称

TaskTempFile::TaskTempFile()
:m_pLog(base::Log::Instance())
{
}

TaskTempFile::~TaskTempFile()
{
	base::Log::Release();
}

bool TaskTempFile::Init(const std::string& path)
{
	m_filePathName.clear();
	if ( path.empty() )
	{
		m_pLog->Output("[ERROR] [TaskTempFile] 初始化失败：临时目录为空！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
		return false;
	}

	if ( path[path.size()-1] != '/' )
	{
		m_filePathName = path + "/" + S_PREFIX_TEMP_FILE + base::SimpleTime::Now().Time14();
	}
	else
	{
		m_filePathName = path + S_PREFIX_TEMP_FILE + base::SimpleTime::Now().Time14();
	}

	if ( !RebuildFile(false) )
	{
		m_pLog->Output("[ERROR] [TaskTempFile] 无法新建任务临时文件！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
		return false;
	}

	try
	{
		m_cfg.SetCfgFile(m_filePathName);
		m_cfg.RegisterItem(S_STATE_SEGMENT, S_PAUSE_ITEM);
	}
	catch ( const base::Exception& ex )		// 异常
	{
		m_pLog->Output("[ERROR] [TaskTempFile] 注册 PAUSE 配置项异常：%s, ERROR_CODE: %d [FILE:%s, LINE:%d]", ex.What().c_str(), ex.ErrorCode(), __FILE__, __LINE__);
		return false;
	}

	return true;
}

bool TaskTempFile::GetStatePause()
{
	if ( !RebuildFile() )
	{
		m_pLog->Output("[ERROR] [TaskTempFile] 无法重建任务临时文件！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
		return false;
	}

	try
	{
		m_cfg.ReadConfig();
		return m_cfg.GetCfgBoolVal(S_STATE_SEGMENT, S_PAUSE_ITEM);
	}
	catch ( const base::Exception& ex )		// 异常：无法成功获取状态配置
	{
		m_pLog->Output("[ERROR] [TaskTempFile] 获取 PAUSE 状态异常：%s, ERROR_CODE: %d [FILE:%s, LINE:%d]", ex.What().c_str(), ex.ErrorCode(), __FILE__, __LINE__);
		return false;
	}
}

bool TaskTempFile::RebuildFile(bool check_exist /*= true*/)
{
	if ( m_filePathName.empty() )
	{
		m_pLog->Output("[ERROR] [TaskTempFile] 重建任务临时文件失败：The temp file name is not initialized! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		return false;
	}

	// 文件是否已经存在？
	if ( check_exist && base::BaseFile::IsFileExist(m_filePathName) )
	{
		return true;
	}

	base::BaseFile bf;
	if ( !bf.Open(m_filePathName, true) )
	{
		m_pLog->Output("[ERROR] [TaskTempFile] 打开任务临时文件失败：%s [FILE:%s, LINE:%d]", m_filePathName.c_str(), __FILE__, __LINE__);
		return false;
	}
	m_pLog->Output("[TaskTempFile] Create task temp file: %s", m_filePathName.c_str());

	if ( !bf.ReadyToWrite() )
	{
		m_pLog->Output("[ERROR] [TaskTempFile] 无法写入任务临时文件：%s [FILE:%s, LINE:%d]", m_filePathName.c_str(), __FILE__, __LINE__);
		return false;
	}

	bf.Write("# "+m_filePathName+": Task temporary file, was created automatically.");
	bf.Write("# For user setting");
	bf.Write("[STATE]");
	bf.Write("PAUSE=NO");
	bf.Close();
	return true;
}

