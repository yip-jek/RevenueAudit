#include "basedir.h"
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "pubstr.h"

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

bool BaseDir::CreateFullPath(const std::string& path)
{
	std::string full_path = PubStr::TrimB(path);
	if ( full_path.empty() )	// 空路径
	{
		return false;
	}

	// 是否存在双斜杠？
	if ( full_path.find("//") != std::string::npos )
	{
		return false;
	}

	return MakeDirRecursive(full_path);
}

bool BaseDir::MakeDirRecursive(const std::string& dir_path)
{
	if ( dir_path.empty() )		// 空目录：Do nothing!
	{
		return true;
	}
	else
	{
		size_t pos_last_slash = dir_path.rfind('/');
		if ( pos_last_slash != std::string::npos )	// 存在父目录
		{
			// 先创建父目录
			if ( !MakeDirRecursive(dir_path.substr(0, pos_last_slash)) )
			{
				return false;
			}

			// 再创建子目录
			return MakeDir(dir_path);
		}
		else	// 不存在父目录
		{
			return MakeDir(dir_path);
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

