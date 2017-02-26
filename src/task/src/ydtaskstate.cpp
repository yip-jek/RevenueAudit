#include "ydtaskstate.h"
#include "basefile.h"
#include "simpletime.h"

const char* const YDTaskState::PREFIX_TMP_FILE = "YDTASK_TEMP_";		// 临时文件名的前缀

YDTaskState::YDTaskState()
:m_timer(MIN_CHECK_TIME)
,m_pause(false)
,m_frozen(false)
{
}

YDTaskState::~YDTaskState()
{
}

void YDTaskState::Clear()
{
	m_pause  = false;
	m_frozen = false;
}

bool YDTaskState::Init(const std::string& path)
{
	Clear();

	m_tmpFile = path + PREFIX_TMP_FILE + base::SimpleTime::Now().Time14();
	if ( !InitTempFile() )
	{
		return false;
	}

	try
	{
		m_tmpCfg.SetCfgFile(m_tmpFile);
		m_tmpCfg.RegisterItem("STATE", "PAUSE");
	}
	catch ( ... )
	{
		return false;
	}

	return m_timer.Start();
}

bool YDTaskState::InitTempFile()
{
	base::BaseFile bf_tmp;
	if ( !bf_tmp.Open(m_tmpFile, true) )
	{
		return false;
	}

	if ( !bf_tmp.ReadyToWrite() )
	{
		return false;
	}

	bf_tmp.Write("# "+m_tmpFile+": YDTask temporary file, was created automatically.");
	bf_tmp.Write("# For user setting");
	bf_tmp.Write("[STATE]");
	bf_tmp.Write("PAUSE=NO");
	bf_tmp.Close();

	return true;
}

void YDTaskState::Check()
{
	CheckFrozen();

	if ( m_timer.IsTimeUp() )
	{
		m_tmpCfg.ReadConfig();
		m_pause = m_tmpCfg.GetCfgBoolVal("STATE", "PAUSE");
	}
}

void YDTaskState::CheckFrozen()
{
	// 每天夜晚 23:30 至次日凌晨 00:00 为冻结期
	// 防止跨日时，时间换算出现不正确的情况！
	const base::SimpleTime ST_NOW(base::SimpleTime::Now());
	if ( m_frozen )	// 已冻结
	{
		if ( ST_NOW.GetHour() != 23 )	// 解冻
		{
			m_frozen = false;
		}
	}
	else	// 未冻结
	{
		if ( ST_NOW.GetHour() == 23 && ST_NOW.GetMin() >= 30 )	// 冻结
		{
			m_frozen = true;
		}
	}
}

