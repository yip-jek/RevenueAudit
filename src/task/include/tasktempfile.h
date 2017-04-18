#pragma once

#include "config.h"

namespace base
{
class Log;
}

// 任务临时文件
class TaskTempFile
{
public:
	TaskTempFile();
	virtual ~TaskTempFile();

	static const char* const S_PREFIX_TEMP_FILE;		// 临时文件名的前缀
	static const char* const S_STATE_SEGMENT;			// 配置：状态段名称
	static const char* const S_PAUSE_ITEM;				// 配置：暂停项名称

public:
	// 初始化
	bool Init(const std::string& path);

	// 获取暂停状态
	bool GetStatePause();

private:
	// 重建临时文件
	// check_exist: 是否检查文件存在性
	bool RebuildFile(bool check_exist = true);

private:
	base::Log*   m_pLog;
	std::string  m_filePathName;		// 文件名（含路径）
	base::Config m_cfg;					// 文件配置
};

