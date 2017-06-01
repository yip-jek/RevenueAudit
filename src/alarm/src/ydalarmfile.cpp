#include "ydalarmfile.h"
#include "basedir.h"
#include "basefile.h"
#include "exception.h"
#include "log.h"
#include "alarmerror.h"

YDAlarmFile::YDAlarmFile()
:m_pLog(base::Log::Instance())
{
}

YDAlarmFile::~YDAlarmFile()
{
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
		throw base::Exception(ALMERR_AFILE_SET_FILE_FMT, "The file format is a blank! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}
}

