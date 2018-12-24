#pragma once

#include <set>
#include <thread>
#include "simpletime.h"

namespace base
{

// 计时器接口类
class TimerInterface
{
public:
	TimerInterface() {}
	virtual ~TimerInterface() {}

public:
	// 接收通知消息
	virtual void Notify() = 0;
};

// 时间间隔计时器
class IntervalTimer
{
public:
	IntervalTimer();
	~IntervalTimer();

	static const char* const S_MONTH_TYPE_W;			// 月间隔类型
	static const char* const S_MONTH_TYPE_F;			// 月间隔类型
	static const char* const S_DAY_TYPE_W;				// 日间隔类型
	static const char* const S_DAY_TYPE_F;				// 日间隔类型
	static const char* const S_HOUR_TYPE_W;				// 小时间隔类型
	static const char* const S_HOUR_TYPE_F;				// 小时间隔类型
	static const char* const S_MINUTE_TYPE_W;			// 分钟间隔类型
	static const char* const S_MINUTE_TYPE_F;			// 分钟间隔类型

	// 时间间隔类型
	enum IntervalType
	{
		IT_UNKNOWN,			// 未知
		IT_MONTH,			// 月间隔
		IT_DAY,				// 日间隔
		IT_HOUR,			// 小时间隔
		IT_MINUTE			// 分钟间隔
	};

public:
	typedef int (*pFunDifferTime)(const SimpleTime&, const SimpleTime&);

	// 是否有效的时间间隔类型
	static bool IsIntervalTypeValid(IntervalType type);

	// 月时间之差
	static int DifferMonthTime(const SimpleTime& st_beg, const SimpleTime& st_end);

	// 日时间之差
	static int DifferDayTime(const SimpleTime& st_beg, const SimpleTime& st_end);

	// 小时时间之差
	static int DifferHourTime(const SimpleTime& st_beg, const SimpleTime& st_end);

	// 分钟时间之差
	static int DifferMinuteTime(const SimpleTime& st_beg, const SimpleTime& st_end);

public:
	// 开始计时
	bool Start();

	// 结束计时
	bool Stop();

	// 运行状态
	bool IsRunning() const;

	// 设置时间间隔类型
	bool SetIntervalType(IntervalType type);
	bool SetIntervalType(const std::string& str_type);

	// 获取时间字串
	std::string GetIntervalTypeTime() const;

	// 注册计时器接口
	void Register(TimerInterface* pTimer);

	// 注销计时器接口
	void Unregister(TimerInterface* pTimer);

private:
	// 执行计时
	void Run();

	// 检查是否到达时间间隔
	bool CheckTimeInterval();

	// 下发通知消息
	void SendNotification();

private:
	bool                      m_bRunning;				// 是否运行
	SimpleTime                m_sRecordTime;			// 记录时间点
	IntervalType              m_iType;					// 时间间隔类型
	pFunDifferTime            m_pfDifferTime;			// 时间差值
	std::thread*              m_pThread;				// 线程
	std::set<TimerInterface*> m_sTimers;				// 计时器接口集
};

}	// namespace base

