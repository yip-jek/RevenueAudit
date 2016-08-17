#include "basedir.h"
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>

namespace base
{

BaseDir::BaseDir(const std::string& path /*= std::string()*/)
:m_pDir(NULL)
{
	SetPath(path);
}

BaseDir::~BaseDir()
{
	Close();
}

void BaseDir::DirWithSlash(std::string& dir)
{
	if ( dir.empty() )
	{
		dir = "/";
	}
	else
	{
		if ( dir[dir.size()-1] != '/' )
		{
			dir.append("/");
		}
	}
}

bool BaseDir::IsDirExist(const std::string& dir_path)
{
	return (0 == access(dir_path.c_str(), F_OK));
}

bool BaseDir::MakeDir(const std::string& dir_path)
{
	if ( !IsDirExist(dir_path) )
	{
		if ( mkdir(dir_path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) != 0 )
		{
			return false;
		}
	}

	return true;
}

bool BaseDir::MakeDirRecursive(const std::string& dir_path)
{
	// 自动建日志路径
	std::vector<std::string> vec_path;
	PubStr::Str2StrVector(path, "/", vec_path);

	// 去除末尾空白
	if ( vec_path[vec_path.size()-1].empty() )
	{
		vec_path.erase(vec_path.end()-1);
	}

	std::string str_path;
	const int VEC_SIZE = vec_path.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		std::string& ref_path = vec_path[i];

		if ( i != 0 )
		{
			if ( ref_path.empty() )
			{
				throw Exception(LG_FILE_PATH_INVALID, "[LOG] The log path is invalid: %s [FILE:%s, LINE:%d]", path.c_str(), __FILE__, __LINE__);
			}

			str_path += "/" + ref_path;

			if ( !TryMakeDir(str_path) )
			{
				throw Exception(LG_FILE_PATH_INVALID, "[LOG] The log path is invalid: %s [FILE:%s, LINE:%d]", path.c_str(), __FILE__, __LINE__);
			}
		}
		else
		{
			if ( !ref_path.empty() )		// 非绝对路径
			{
				str_path += ref_path;

				if ( !TryMakeDir(str_path) )
				{
					throw Exception(LG_FILE_PATH_INVALID, "[LOG] The log path is invalid: %s [FILE:%s, LINE:%d]", path.c_str(), __FILE__, __LINE__);
				}
			}
		}
	}
}

bool BaseDir::SetPath(const std::string& path)
{
	if ( path.empty() )
	{
		return false;
	}

	m_strPath = path;

	// 加上末尾的斜杠
	DirWithSlash(m_strPath);

	return true;
}

bool BaseDir::Open(std::string* pStrError)
{
	Close();

	m_pDir = opendir(m_strPath.c_str());
	if ( NULL == m_pDir )
	{
		// 设置错误信息
		if ( pStrError != NULL )
		{
			pStrError->assign(strerror(errno));
		}

		return false;
	}

	return true;
}

void BaseDir::Close()
{
	if ( m_pDir != NULL )
	{
		closedir(m_pDir);
		m_pDir = NULL;
	}
}

bool BaseDir::GetFileName(std::string& file_name)
{
	struct dirent* p_dnt = NULL;
	struct stat st;
	std::string str_fullname;

	while ( (p_dnt = readdir(m_pDir)) != NULL )
	{
		// 忽略隐藏文件
		if ( 0 == p_dnt->d_ino || '.' == p_dnt->d_name[0] )
		{
			continue;
		}

		str_fullname = m_strPath + p_dnt->d_name;

		// 获取文件信息失败，跳过
		if ( stat(str_fullname.c_str(), &st) != 0 )
		{
			continue;
		}

		// 忽略目录
		if ( S_ISDIR(st.st_mode) )
		{
			continue;
		}

		file_name = p_dnt->d_name;
		return true;
	}

	return false;
}

bool BaseDir::GetFullName(std::string& full_name)
{
	if ( GetFileName(full_name) )
	{
		full_name = m_strPath + full_name;
		return true;
	}

	return false;
}

}	// namespace base

