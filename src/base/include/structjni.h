#pragma once

#include <string>
#include "jni.h"

namespace base
{

// JNI 接口结构体
struct StructJNI
{
private:    // noncopyable
	StructJNI(const StructJNI& );
	const StructJNI& operator = (const StructJNI& );

public:
	StructJNI();

	static const int S_JVM_OPTIONS = 3;			// JVM option 个数

public:
	// 转换：jstring -> std::string
	// 谨慎调用：JNIEnv* p_jni_env 需先初始化完成
	std::string JString2String(jstring& j_str);

public:
	JavaVMInitArgs	jvm_args;
	JavaVMOption	jvm_option[S_JVM_OPTIONS];
	JavaVM*			p_jvm;
	JNIEnv*			p_jni_env;

public:
	jobject			jobj_hiveagent;

public:
	jmethodID		jmid_set_zkquorum;
	jmethodID		jmid_set_krb5conf;
	jmethodID		jmid_set_usrkeytab;
	jmethodID		jmid_set_principal;
	jmethodID		jmid_set_jaasconf;
	jmethodID		jmid_init;
	jmethodID		jmid_connect;
	jmethodID		jmid_disconnect;
	jmethodID		jmid_is_connected;
	jmethodID		jmid_get_error_msg;
	jmethodID		jmid_execute_sql;
	jmethodID		jmid_fetch_data;

public:
	jmethodID		jmid_array_size;
	jmethodID		jmid_array_get;
};

}	// namespace base

