#pragma once

#include "config.h"
#include "sectimer.h"

// 任务状态
class YDTaskState
{
public:
	YDTaskState();
	virtual ~YDTaskState();

	static const long MIN_CHECK_TIME    = 60;			// 最小时间间隔（单位：秒）
	static const char* const PREFIX_TMP_FILE;			// 临时文件名的前缀

public:
	// 初始化
	bool Init(const std::string& path);

	// 检查状态
	void Check();

	// 获取临时文件名
	std::string GetTempFile() const
	{ return m_tmpFile; }

	bool IsPause()  const { return m_pause ; }			// 是否为暂停状态？
	bool IsFrozen() const { return m_frozen; }			// 是否为冻结状态？

private:
	// 清除状态
	void Clear();

	// 初始化临时文件
	bool InitTempFile();

	// 检查冻结状态
	void CheckFrozen();

private:
	SecTimer     m_timer;			// 计时器
	std::string  m_tmpFile;			// 临时文件名
	base::Config m_tmpCfg;			// 临时文件配置

private:
	bool         m_pause;			// 暂停状态
	bool         m_frozen;			// 冻结状态
};

