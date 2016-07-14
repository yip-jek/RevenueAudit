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

