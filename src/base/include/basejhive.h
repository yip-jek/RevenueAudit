#pragma once

#include <string>
#include <vector>
#include "exception.h"

namespace base
{

class Log;
struct StructJNI;

// 基础 JHive 类
// 通过 jni 调用 Java 的 HiveAgent 接口，实现对 HIVE 的访问
class BaseJHive
{
public:
	BaseJHive(const std::string& host, int port, const std::string& usr, const std::string& pwd);
	virtual ~BaseJHive();

public:
	// 初始化
	// 输入：
	// 	load_jar_path：依赖的 jar 包的路径（建议使用绝对路径）
	virtual void Init(const std::string& load_jar_path) throw(Exception);

	// 连接 Hive Server
	virtual void Connect() throw(Exception);

	// 断开 Hive Server 的连接
	virtual bool Disconnect();

	// 查询状态：是否已连接
	virtual bool IsConnected();

	// 获取最后一次的错误信息
	virtual std::string GetErrorMsg();

	// 执行 HIVE SQL 语句
	virtual void ExecuteSQL(const std::string& sql) throw(Exception);

	// 获取 HIVE 数据
	virtual void FetchSQL(const std::string& sql, std::vector<std::vector<std::string> >& vec2_data) throw(Exception);

protected:
	// 获取 jar 包的 classpath
	// 输入：
	// 	load_jar_path：依赖的 jar 包的路径（建议使用绝对路径）
	virtual std::string GetJarClasspath(const std::string& load_jar_path) throw(Exception);

	// 创建 Java 虚拟机
	// 输入：
	// 	load_jar_path：依赖的 jar 包的路径（建议使用绝对路径）
	virtual void CreateJVM(const std::string& load_jar_path) throw(Exception);

	// 销毁 Java 虚拟机
	virtual void DestroyJVM();

	// 初始化 (Java) HiveAgent
	virtual void InitHiveAgent() throw(Exception);

protected:
	Log*		m_pLog;				// 日志

protected:
	StructJNI*	m_pJNI;				// JNI 接口

protected:
	std::string	m_strHost;			// HIVE 主机信息
	int			m_nPort;			// HIVE 主机 hiveserver2 的端口号
	std::string	m_strUsr;			// HIVE 主机用户名
	std::string	m_strPwd;			// HIVE 主机密码
};

}	// namespace base

