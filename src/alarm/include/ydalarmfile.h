#pragma once

#include "exception.h"

namespace base
{

class Log;

}

// 告警（短信）文件
class YDAlarmFile
{
public:
	YDAlarmFile();
	~YDAlarmFile();

public:
	// 设置文件路径
	void SetPath(const std::string& path) throw(base::Exception);

	// 设置文件格式
	void SetFileFormat(const std::string& file_fmt) throw(base::Exception);

	// 设置文件最大行数
	void SetMaxLine(int max_line) throw(base::Exception);

private:
	base::Log*  m_pLog;

private:
	std::string m_filePath;			// 文件所在路径
	std::string m_fileFmt;			// 文件名格式
	int         m_maxLine;			// 文件最大行数
};

