#pragma once

#include <string>
#include <fstream>

namespace base
{

// 基础文件类
class BaseFile
{
private:    // noncopyable
	BaseFile(const BaseFile& );
	const BaseFile& operator = (const BaseFile& );

public:
	// 构造：打开文件默认以追加的方式
	explicit BaseFile(const std::string& file_path = std::string());
	virtual ~BaseFile();

	// 文件是否存在
	static bool IsFileExist(const std::string& file_path);

public:
	// 打开文件
	// 输入参数 trunc 表示是否清空已存在文件的内容
	virtual bool Open(const std::string& file_path, bool trunc = false);

	// 关闭文件
	virtual void Close();

	// 文件是否正常打开
	virtual bool IsOpen() const;

	// 是否是文件的末尾
	virtual bool IsEOF() const;

	// 文件读取前的准备
	virtual bool ReadyToRead();

	// 读取文件一行内容
	virtual bool Read(std::string& read_line);

	// 文件写入前的准备
	virtual bool ReadyToWrite();

	// 写入一行内容到文件
	virtual void Write(const std::string& write_line);

	// 获取文件的大小
	virtual unsigned long long FileSize();

	// 文件路径
	virtual std::string GetFilePath() const;

protected:
	// 重置状态
	virtual void ResetState();

	// 准备文件的读写
	// 输入参数 read_or_write: true-表示读取，false-表示写入
	virtual bool ReadWriteReady(bool read_or_write);

protected:
	std::string		m_strFilePath;			// 文件路径
	std::fstream	m_fsFile;				// 文件流
};

}	// namespace base

