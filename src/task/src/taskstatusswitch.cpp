#include "taskstatusswitch.h"
#include "simpletime.h"

TaskStatusSwitch::TaskStatusSwitch()
:m_timer(S_CHK_TEMPFILE_INTERVAL)
,m_pause(false)
,m_frozen(false)
{
}

TaskStatusSwitch::~TaskStatusSwitch()
{
}

void TaskStatusSwitch::Clear()
{
	m_pause  = false;
	m_frozen = false;
}

bool TaskStatusSwitch::Init(const std::string& temp_path)
{
	Clear();

	return (m_tmpFile.Init(temp_path) && m_timer.Start());
}

void TaskStatusSwitch::Check()
{
	CheckFrozenState();

	if ( m_timer.IsTimeUp() )
	{
		m_pause = m_tmpFile.GetStatePause();
	}
}

void TaskStatusSwitch::CheckFrozenState()
{
	// 每天夜晚 23:30 至次日凌晨 00:00 为冻结期
	// 防止跨日时，时间换算出现不正确的情况！
	const base::SimpleTime ST_NOW(base::SimpleTime::Now());
	if ( m_frozen )	// 已冻结
	{
		if ( ST_NOW.GetHour() != S_FREEZE_HOUR )	// 解冻
		{
			m_frozen = false;
		}
	}
	else	// 未冻结
	{
		if ( ST_NOW.GetHour() == S_FREEZE_HOUR && ST_NOW.GetMin() >= S_FREEZE_MINUTE )	// 冻结
		{
			m_frozen = true;
		}
	}
}

