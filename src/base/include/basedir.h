#pragma once

#include <string>

typedef struct __dirstream DIR;

namespace base
{

// 基础 Directory 类
class BaseDir
{
private:    // noncopyable
	BaseDir(const BaseDir& );
	const BaseDir& operator = (const BaseDir& );

public:
	explicit BaseDir(const std::string& path = std::string());
	virtual ~BaseDir();

	// 目录末尾加上斜杠
	static void DirWithSlash(std::string& dir);

public:
	// 设置目录
	virtual bool SetPath(const std::string& path);

	// 打开目录
	// std::string* pStrError：接收错误信息
	virtual bool Open(std::string* pStrError);

	// 关闭目录
	virtual void Close();

	// 获取目录下的文件名
	virtual bool GetFileName(std::string& file_name);

	// 获取目录下的文件全名（包含目录）
	virtual bool GetFullName(std::string& full_name);

protected:
	DIR*		m_pDir;
	std::string	m_strPath;
};

}	// namespace base

