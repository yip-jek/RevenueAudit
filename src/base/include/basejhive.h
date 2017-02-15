#pragma once

#include <string>
#include <vector>
#include "exception.h"

namespace base
{

class Log;
struct StructJNI;

// 基础 JHive 类
// 通过 jni 调用 Java 的 HiveAgent/HiveAgentTest 接口，实现对 HIVE 的访问
class BaseJHive
{
public:
	explicit BaseJHive(const std::string& hive_jclassname);
	virtual ~BaseJHive();

	static const char* const S_DEBUG_HIVE_JAVA_CLASS_NAME;				// HIVE代理Java类名称（测试版本）
	static const char* const S_RELEASE_HIVE_JAVA_CLASS_NAME;			// HIVE代理Java类名称（发布版本）

	// 设置 Java 虚拟机初始化内存大小（单位：MB）
	static bool SetJVMInitMemSize(unsigned int mem_size);

	// 设置 Java 虚拟机最大内存大小（单位：MB）
	static bool SetJVMMaxMemSize(unsigned int mem_size);

public:
	// 设置
	virtual bool SetZooKeeperQuorum(const std::string& zk_quorum);
	virtual bool SetKrb5Conf(const std::string& krb5_conf);
	virtual bool SetUserKeytab(const std::string& usr_keytab);
	virtual bool SetPrincipal(const std::string& principal);
	virtual bool SetJaasConf(const std::string& jaas_conf);

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
	static unsigned int s_jvm_init_mem_size;			// Java 虚拟机初始化内存大小（单位：MB）
	static unsigned int s_jvm_max_mem_size;				// Java 虚拟机最大内存大小（单位：MB）

protected:
	Log*       m_pLog;				// 日志
	StructJNI* m_pJNI;				// JNI 接口

protected:
	std::string m_hiveJClassName;				// Hive代理的Java接口类名
	std::string m_zk_quorum;
};

}	// namespace base

