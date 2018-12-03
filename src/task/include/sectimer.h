#pragma once

// 简单（秒）计时器
class SecTimer
{
public:
	explicit SecTimer(long sec);
	virtual ~SecTimer();

public:
	// 设置
	bool Set(long sec);

	// 开始
	bool Start();

	// 检查：是否已经到时间
	bool IsTimeUp();

	// 等候：时间到达
	void WaitForTimeUp();

private:
	long m_sectime;
	long m_curtime;
};

