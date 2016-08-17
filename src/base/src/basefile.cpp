#include "basefile.h"
#include <stdio.h>
#include "pubstr.h"

namespace base
{

BaseFile::BaseFile(const std::string& file_path /*= std::string()*/)
{
	if ( !file_path.empty() )
	{
		Open(file_path);
	}
}

BaseFile::~BaseFile()
{
	Close();
}

bool BaseFile::IsFileExist(const std::string& file_path)
{
	return (0 == access(file_path.c_str(), 0));
}

bool BaseFile::Open(const std::string& file_path, bool trunc /*= false*/)
{
	Close();

	m_strFilePath = PubStr::TrimB(file_path);
	if ( trunc )	// 新建方式
	{
		m_fsFile.open(m_strFilePath.c_str(), std::fstream::in|std::fstream::out|std::fstream::trunc);
	}
	else	// 追加方式
	{
		m_fsFile.open(m_strFilePath.c_str(), std::fstream::in|std::fstream::out|std::fstream::app);
	}

	if ( !IsOpen() )	// 打开失败
	{
		m_strFilePath.clear();
		return false;
	}
	return true;
}

void BaseFile::Close()
{
	if ( IsOpen() )
	{
		m_fsFile.close();
	}
}

bool BaseFile::IsOpen() const
{
	return m_fsFile.is_open();
}

bool BaseFile::IsEOF() const
{
	return m_fsFile.eof();
}

bool BaseFile::ReadyToRead()
{
	return ReadWriteReady(true);
}

bool BaseFile::Read(std::string& read_line)
{
	read_line.clear();

	if ( !IsEOF() )
	{
		std::getline(m_fsFile, read_line);
		return true;
	}

	return false;
}

bool BaseFile::ReadyToWrite()
{
	return ReadWriteReady(false);
}

void BaseFile::Write(const std::string& write_line)
{
	const std::string WR_LINE = write_line + "\n";
	m_fsFile.write(WR_LINE.c_str(), WR_LINE.size());
	m_fsFile.flush();
}

unsigned long long BaseFile::FileSize()
{
	ResetState();

	m_fsFile.seekg(0, std::ios::end);

	return m_fsFile.tellg();
}

std::string BaseFile::GetFilePath() const
{
	return m_strFilePath;
}

void BaseFile::ResetState()
{
	if ( m_fsFile.fail() )
	{
		m_fsFile.clear();
	}
}

bool BaseFile::ReadWriteReady(bool read_or_write)
{
	if ( IsOpen() )
	{
		ResetState();

		if ( read_or_write )	// 读取
		{
			m_fsFile.seekg(0, std::ios::beg);
		}
		else	// 写入
		{
			m_fsFile.seekp(0, std::ios::end);
		}
		return true;
	}

	return false;
}

}	// namespace base

