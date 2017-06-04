#include "ydalarmfile.h"
#include "basedir.h"
#include "log.h"
#include "pubstr.h"
#include "simpletime.h"
#include "sqltranslator.h"
#include "alarmerror.h"

const char* const YDAlarmFile::S_NOW_DAY       = "NOW_DAY";				// 当前（系统时间）天：YYYYMMDD
const char* const YDAlarmFile::S_NOW_TIMESTAMP = "NOW_TIMESTAMP";		// 当前（系统时间）时间戳：YYYYMMDDHHMISS
const char* const YDAlarmFile::S_SERIAL_NUM    = "SERIAL";				// 序号，从 0 开始

YDAlarmFile::YDAlarmFile()
:m_pLog(base::Log::Instance())
,m_maxLine(0)
,m_lineCount(0)
{
}

YDAlarmFile::~YDAlarmFile()
{
	CloseAlarmFile();
	base::Log::Release();
}

void YDAlarmFile::SetPath(const std::string& path) throw(base::Exception)
{
	if ( path.empty() )
	{
		throw base::Exception(ALMERR_AFILE_SET_PATH, "The alarm file path is a blank! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 存放告警短信文件的路径是否存在
	if ( !base::BaseDir::IsDirExist(path) )
	{
		// 路径不存在，则尝试创建
		if ( !base::BaseDir::CreateFullPath(path) )
		{
			throw base::Exception(ALMERR_AFILE_SET_PATH, "Try to create alarm file path failed! The file path is invalid: PATH=[%s] [FILE:%s, LINE:%d]", path.c_str(), __FILE__, __LINE__);
		}

		m_pLog->Output("[YDAlarmFile] Created alarm file path: [%s]", path.c_str());
	}

	m_filePath = path;
	base::BaseDir::DirWithSlash(m_filePath);
}

void YDAlarmFile::SetFileFormat(const std::string& file_fmt) throw(base::Exception)
{
	if ( file_fmt.empty() )
	{
		throw base::Exception(ALMERR_AFILE_SET_FILE_FMT, "The alarm file format is a blank! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 验证文件名格式是否正确
	std::string file_name;
	if ( TryMakeFileNameByFormat(file_fmt, 0, file_name) )
	{
		m_fileFmt = base::PubStr::TrimB(file_fmt);
	}
	else
	{
		throw base::Exception(ALMERR_AFILE_SET_FILE_FMT, "The alarm file format is invalid: %s [FILE:%s, LINE:%d]", file_fmt.c_str(), __FILE__, __LINE__);
	}
}

void YDAlarmFile::SetMaxLine(int max_line) throw(base::Exception)
{
	if ( max_line > 0 )
	{
		m_maxLine = max_line;
	}
	else
	{
		throw base::Exception(ALMERR_AFILE_SET_MAX_LINE, "The alarm file max line is invalid: %d [FILE:%s, LINE:%d]", max_line, __FILE__, __LINE__);
	}
}

bool YDAlarmFile::TryMakeFileNameByFormat(const std::string& fmt, int serial, std::string& file_name)
{
	std::string make_file_name = fmt;
	std::string mark;
	std::string tup_mark;
	size_t      pos    = 0;
	size_t      mk_pos = 0;

	while ( base::SQLTranslator::GetMark(make_file_name, mark, pos) )
	{
		mk_pos   = mark.size() + 3;
		tup_mark = base::PubStr::TrimUpperB(mark);

		if ( S_NOW_DAY == tup_mark )		// 当前天
		{
			make_file_name.replace(pos, mk_pos, base::SimpleTime::Now().DayTime8());
		}
		else if ( S_NOW_TIMESTAMP == tup_mark )		// 当前时间戳
		{
			make_file_name.replace(pos, mk_pos, base::SimpleTime::Now().Time14());
		}
		else	// Other
		{
			std::vector<std::string> vec_str;
			base::PubStr::Str2StrVector(tup_mark, ":", vec_str);

			// 是否为序号格式：$(SERIAL:[n])
			int serial_width = 0;
			if ( (int)vec_str.size() == 2 && S_SERIAL_NUM == vec_str[0] 
				&& base::PubStr::Str2Int(vec_str[1], serial_width) && serial_width > 0 )
			{
				std::string str_fmt;
				std::string str_serial;
				base::PubStr::SetFormatString(str_fmt, "%%0%dd", serial_width);
				base::PubStr::SetFormatString(str_serial, str_fmt.c_str(), serial);

				make_file_name.replace(pos, mk_pos, str_serial);
			}
			else
			{
				m_pLog->Output("[YDAlarmFile] Invalid symbol in alarm file format: $(%s)", mark.c_str());
				return false;
			}
		}
	}

	file_name = make_file_name;
	return true;
}

void YDAlarmFile::OpenNewAlarmFile() throw(base::Exception)
{
	CloseAlarmFile();

	std::string new_file_name = GetNewAlarmFileName();
	m_pLog->Output("[YDAlarmFile] Open alarm file: [%s]", new_file_name.c_str());

	if ( !m_alarmFile.Open(new_file_name, true) )
	{
		throw base::Exception(ALMERR_AFILE_OPEN_NEWFILE, "Open alarm file failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( !m_alarmFile.ReadyToWrite() )
	{
		throw base::Exception(ALMERR_AFILE_OPEN_NEWFILE, "Ready to write alarm file failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_lineCount = 0;
}

void YDAlarmFile::CloseAlarmFile()
{
	if ( m_alarmFile.IsOpen() )
	{
		m_pLog->Output("[YDAlarmFile] Close alarm file: [%s]", m_alarmFile.GetFilePath().c_str());
		m_alarmFile.Close();
	}
}

void YDAlarmFile::WriteAlarmData(const std::string& alarm_data)
{
	m_alarmFile.Write(alarm_data+"\r");

	// 累计行数，并判断是否达到最大行数
	// 若达到最大行数，则打开新的文件
	if ( (++m_lineCount) >= m_maxLine )
	{
		m_pLog->Output("[YDAlarmFile] 告警文件行数达到最大值上限：[%d]", m_lineCount);
		OpenNewAlarmFile();
	}
}

std::string YDAlarmFile::GetNewAlarmFileName()
{
	int         serial = 0;
	std::string file_name;

	// 生成不存在的新文件名
	do
	{
		TryMakeFileNameByFormat(m_fileFmt, serial++, file_name);
		file_name = m_filePath + file_name;
	}
	while ( base::BaseFile::IsFileExist(file_name) );

	return file_name;
}

