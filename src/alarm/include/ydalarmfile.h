#pragma once

#include "exception.h"
#include "basefile.h"

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

	static const char* const S_NOW_DAY;				// 当前（系统时间）天：YYYYMMDD
	static const char* const S_NOW_TIMESTAMP;		// 当前（系统时间）时间戳：YYYYMMDDHHMISS
	static const char* const S_SERIAL_NUM;			// 序号，从 0 开始

public:
	// 设置文件路径
	void SetPath(const std::string& path);

	// 设置文件名格式
	void SetFileFormat(const std::string& file_fmt);

	// 设置文件最大行数
	void SetMaxLine(int max_line);

	// 打开新的告警（短信）文件
	void OpenNewAlarmFile();

	// 关闭告警（短信）文件
	void CloseAlarmFile();

	// 写入告警数据
	void WriteAlarmData(const std::string& alarm_data);

private:
	// 以文件名格式生成文件名
	bool TryMakeFileNameByFormat(const std::string& fmt, int serial, std::string& file_name);

	// 获取新的文件名
	std::string GetNewAlarmFileName();

private:
	base::Log*     m_pLog;
	base::BaseFile m_alarmFile;

private:
	std::string m_filePath;			// 文件所在路径
	std::string m_fileFmt;			// 文件名格式
	int         m_maxLine;			// 文件最大行数
	int         m_lineCount;		// 文件行计数
};

