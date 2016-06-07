#include "basejhive.h"
#include <string.h>
#include <stdlib.h>
#include "def.h"
#include "log.h"
#include "structjni.h"
#include "basedir.h"
#include "pubstr.h"


namespace base
{

BaseJHive::BaseJHive(const std::string& host, int port, const std::string& usr, const std::string& pwd)
:m_pLog(Log::Instance())
,m_pJNI(NULL)
,m_strHost(host)
,m_nPort(port)
,m_strUsr(usr)
,m_strPwd(pwd)
{
}

BaseJHive::~BaseJHive()
{
	if ( m_pJNI != NULL )
	{
		DestroyJVM();

		delete m_pJNI;
		m_pJNI = NULL;
	}

	Log::Release();
}

void BaseJHive::Init(const std::string& load_jar_path) throw(Exception)
{
	// JNI 接口是否已经初始化？
	if ( m_pJNI != NULL )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] Already initialized! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 初始化 JNI 接口
	m_pJNI = new StructJNI();
	if ( NULL == m_pJNI )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] new StructJNI failed: Allocate memory failed ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	CreateJVM(load_jar_path);

	InitHiveAgent();

	m_pLog->Output("[BASE HIVE] Init OK.");
}

void BaseJHive::Connect() throw(Exception)
{
	if ( NULL == m_pJNI )
	{
		throw Exception(BJH_CONNECT_FAILED, "[BASE HIVE] NOT initialized ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[BASE HIVE] Connect to <%s:%d> ...", m_strHost.c_str(), m_nPort);

	jstring jstr_host = m_pJNI->p_jni_env->NewStringUTF(m_strHost.c_str());
	jstring jstr_usr  = m_pJNI->p_jni_env->NewStringUTF(m_strUsr.c_str());
	jstring jstr_pwd  = m_pJNI->p_jni_env->NewStringUTF(m_strPwd.c_str());

	jint res_conn = m_pJNI->p_jni_env->CallIntMethod(m_pJNI->jobj_hiveagent, m_pJNI->jmid_connect, jstr_host, m_nPort, jstr_usr, jstr_pwd);
	switch ( res_conn )
	{
	case 0:
		m_pLog->Output("[BASE HIVE] Connect <%s:%d> OK.", m_strHost.c_str(), m_nPort);
		break;
	case 1:
	case -1:
	case -2:
		throw Exception(BJH_CONNECT_FAILED, "[BASE HIVE] Connect failed: %s [FILE:%s, LINE:%d]", GetErrorMsg().c_str(), __FILE__, __LINE__);
		break;
	default:
		throw Exception(BJH_CONNECT_FAILED, "[BASE HIVE] Connect failed: Unknown error - %d [FILE:%s, LINE:%d]", res_conn, __FILE__, __LINE__);
		break;
	}
}

bool BaseJHive::Disconnect()
{
	if ( m_pJNI != NULL )
	{
		if ( static_cast<bool>(m_pJNI->p_jni_env->CallBooleanMethod(m_pJNI->jobj_hiveagent, m_pJNI->jmid_disconnect)) )
		{
			m_pLog->Output("[BASE HIVE] Disconnect OK.");
			return true;
		}
		else
		{
			m_pLog->Output("[BASE HIVE] Disconnect failed: %s", GetErrorMsg().c_str());
			return false;
		}
	}

	return false;
}

bool BaseJHive::IsConnected()
{
	if ( m_pJNI != NULL )
	{
		return static_cast<bool>(m_pJNI->p_jni_env->CallBooleanMethod(m_pJNI->jobj_hiveagent, m_pJNI->jmid_is_connected));
	}

	return false;
}

std::string BaseJHive::GetErrorMsg()
{
	if ( m_pJNI != NULL )
	{
		jstring jstr_errmsg = static_cast<jstring>(m_pJNI->p_jni_env->CallObjectMethod(m_pJNI->jobj_hiveagent, m_pJNI->jmid_get_error_msg));
		return m_pJNI->JString2String(jstr_errmsg);
	}

	return std::string();
}

void BaseJHive::ExecuteSQL(const std::string& sql) throw(Exception)
{
	if ( NULL == m_pJNI )
	{
		throw Exception(BJH_EXECUTE_SQL_FAILED, "[BASE HIVE] NOT initialized ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[BASE HIVE] Execute SQL: %s", sql.c_str());

	jstring jstr_sql = m_pJNI->p_jni_env->NewStringUTF(sql.c_str());
	if ( static_cast<bool>(m_pJNI->p_jni_env->CallBooleanMethod(m_pJNI->jobj_hiveagent, m_pJNI->jmid_execute_sql, jstr_sql)) )
	{
		m_pLog->Output("[BASE HIVE] Execute SQL OK.");
	}
	else
	{
		throw Exception(BJH_EXECUTE_SQL_FAILED, "[BASE HIVE] Execute SQL failed: %s [FILE:%s, LINE:%d]", GetErrorMsg().c_str(), __FILE__, __LINE__);
	}
}

void BaseJHive::FetchSQL(const std::string& sql, std::vector<std::vector<std::string> >& vec2_data) throw(Exception)
{
	if ( NULL == m_pJNI )
	{
		throw Exception(BJH_FETCH_DATA_FAILED, "[BASE HIVE] NOT initialized ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[BASE HIVE] Fetch SQL: %s", sql.c_str());

	jstring jstr_sql = m_pJNI->p_jni_env->NewStringUTF(sql.c_str());
	jobject jobj_arraylist = m_pJNI->p_jni_env->CallObjectMethod(m_pJNI->jobj_hiveagent, m_pJNI->jmid_fetch_data, jstr_sql);
	if ( NULL == jobj_arraylist )
	{
		throw Exception(BJH_FETCH_DATA_FAILED, "[BASE HIVE] Fetch data failed: ArrayList is null ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	const jint ARRAY_SIZE = m_pJNI->p_jni_env->CallIntMethod(jobj_arraylist, m_pJNI->jmid_array_size);
	m_pLog->Output("[BASE HIVE] Fetch data size: %ld", ARRAY_SIZE);

	if ( ARRAY_SIZE <= 0 )
	{
		const std::string ERROR_MSG = GetErrorMsg();

		// 是否有异常信息？
		if ( PubStr::UpperB(ERROR_MSG).find("EXCEPTION") != std::string::npos )
		{
			throw Exception(BJH_FETCH_DATA_FAILED, "[BASE HIVE] Fetch data failed: %s [FILE:%s, LINE:%d]", ERROR_MSG.c_str(), __FILE__, __LINE__);
		}
	}

	std::vector<std::string> v_data;
	std::vector<std::vector<std::string> > vv_data;

	std::string row_str;
	for ( jint i = 0; i < ARRAY_SIZE; ++i )
	{
		jstring jstr_row = static_cast<jstring>(m_pJNI->p_jni_env->CallObjectMethod(jobj_arraylist, m_pJNI->jmid_array_get, i));

		row_str = m_pJNI->JString2String(jstr_row);
		if ( row_str.empty() )
		{
			throw Exception(BJH_FETCH_DATA_FAILED, "[BASE HIVE] Fetch data failed: Source data is a blank! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		}

		PubStr::Str2StrVector(row_str, "\t", v_data);
		PubStr::VVectorSwapPushBack(vv_data, v_data);
	}

	vv_data.swap(vec2_data);

	m_pLog->Output("[BASE HIVE] Fetch data OK.");
}

std::string BaseJHive::GetJarClasspath(const std::string& load_jar_path) throw(Exception)
{
	BaseDir dir_jar;
	if ( !dir_jar.SetPath(load_jar_path) )
	{
		throw Exception(BJH_GET_CLASSPATH_FAILED, "[BASE HIVE] The jar load_path is not set correctly! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	std::string str;
	if ( !dir_jar.Open(&str) )
	{
		throw Exception(BJH_GET_CLASSPATH_FAILED, "[BASE HIVE] Open jar load_path \"%s\" failed: %s ! [FILE:%s, LINE:%d]", load_jar_path.c_str(), str.c_str(), __FILE__, __LINE__);
	}

	std::string str_classpath;
	while ( dir_jar.GetFullName(str) )
	{
		// 是否为 jar 包？
		if ( str.substr(str.size()-4) != ".jar" )
		{
			continue;
		}

		if ( !str_classpath.empty() )
		{
			str_classpath.append(":");
		}

		str_classpath.append(str);
	}

	dir_jar.Close();

	if ( str_classpath.empty() )
	{
		throw Exception(BJH_GET_CLASSPATH_FAILED, "[BASE HIVE] NO jar package found ! [FILE:%s, LINE:%d]", load_jar_path.c_str(), str.c_str(), __FILE__, __LINE__);
	}

	return str_classpath;
}

void BaseJHive::CreateJVM(const std::string& load_jar_path) throw(Exception)
{
	m_pJNI->jvm_args.version = JNI_VERSION_1_6;
	m_pJNI->jvm_args.nOptions = 1;
	m_pJNI->jvm_args.ignoreUnrecognized = JNI_TRUE;

	// 包含当前路径
	std::string str_option = "-Djava.class.path=.:";
	str_option += GetJarClasspath(load_jar_path);

	// 获取 $CLASSPATH 环境变量
	char* pEnvClasspath = getenv("CLASSPATH");
	if ( NULL == pEnvClasspath )
	{
		m_pLog->Output("<WARNING> [BASE HIVE] Get environment variable $CLASSPATH failed !");
	}
	else
	{
		str_option.append(":");
		str_option.append(pEnvClasspath);
	}

	char* pstr_op = new char[str_option.size()+1];
	if ( NULL == pstr_op )
	{
		throw Exception(BJH_CREATE_JVM_FAILED, "[BASE HIVE] new char[] failed: Allocate memory failed ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	strcpy(pstr_op, str_option.c_str());
	m_pJNI->jvm_option[0].optionString = pstr_op;
	m_pJNI->jvm_args.options = m_pJNI->jvm_option;

	m_pLog->Output("[BASE HIVE] JavaVMInitArgs.version            = JNI_VERSION_1_6");
	m_pLog->Output("[BASE HIVE] JavaVMInitArgs.nOptions           = 1");
	m_pLog->Output("[BASE HIVE] JavaVMInitArgs.options            = %s", pstr_op);
	m_pLog->Output("[BASE HIVE] JavaVMInitArgs.ignoreUnrecognized = JNI_TRUE");

	jint res = JNI_CreateJavaVM(&(m_pJNI->p_jvm), (void**)&(m_pJNI->p_jni_env), &(m_pJNI->jvm_args));

	// 释放资源
	delete[] pstr_op;
	pstr_op = NULL;

	if ( res < 0 )
	{
		throw Exception(BJH_CREATE_JVM_FAILED, "[BASE HIVE] Create JavaVM failed: %d [FILE:%s, LINE:%d]", res, __FILE__, __LINE__);
	}

	m_pLog->Output("[BASE HIVE] Create JavaVM OK.");
}

void BaseJHive::DestroyJVM()
{
	if ( m_pJNI->p_jvm != NULL )
	{
		m_pJNI->p_jvm->DestroyJavaVM();

		m_pLog->Output("[BASE HIVE] Destroy JavaVM OK.");
	}
}

void BaseJHive::InitHiveAgent() throw(Exception)
{
	const std::string JAVA_HIVE_CLASS_NAME = "HiveAgent";

	// 找寻 Java HIVE 代理类
	jclass jclass_hiveagent = m_pJNI->p_jni_env->FindClass(JAVA_HIVE_CLASS_NAME.c_str());
	if ( NULL == jclass_hiveagent )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] Can not find Java class: %s [FILE:%s, LINE:%d]", JAVA_HIVE_CLASS_NAME.c_str(), __FILE__, __LINE__);
	}

	// 获取 Java HIVE 代理类的构造方法
	jmethodID mid = m_pJNI->p_jni_env->GetMethodID(jclass_hiveagent, "<init>", "()V");
	if ( NULL == mid )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] NO construct method of %s ! [FILE:%s, LINE:%d]", JAVA_HIVE_CLASS_NAME.c_str(), __FILE__, __LINE__);
	}

	// 构造 Java HIVE 代理类
	m_pJNI->jobj_hiveagent = m_pJNI->p_jni_env->NewObject(jclass_hiveagent, mid);
	if ( NULL == m_pJNI->jobj_hiveagent )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] Construct %s failed ! [FILE:%s, LINE:%d]", JAVA_HIVE_CLASS_NAME.c_str(), __FILE__, __LINE__);
	}

	// 获取 Java HIVE 代理类的 Connect 方法
	m_pJNI->jmid_connect = m_pJNI->p_jni_env->GetMethodID(jclass_hiveagent, "Connect", "(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;)I");
	if ( NULL == m_pJNI->jmid_connect )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] NO connect method of %s ! [FILE:%s, LINE:%d]", JAVA_HIVE_CLASS_NAME.c_str(), __FILE__, __LINE__);
	}

	// 获取 Java HIVE 代理类的 Disconnect 方法
	m_pJNI->jmid_disconnect = m_pJNI->p_jni_env->GetMethodID(jclass_hiveagent, "Disconnect", "()Z");
	if ( NULL == m_pJNI->jmid_disconnect )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] NO disconnect method of %s ! [FILE:%s, LINE:%d]", JAVA_HIVE_CLASS_NAME.c_str(), __FILE__, __LINE__);
	}

	// 获取 Java HIVE 代理类的 IsConnected 方法
	m_pJNI->jmid_is_connected = m_pJNI->p_jni_env->GetMethodID(jclass_hiveagent, "IsConnected", "()Z");
	if ( NULL == m_pJNI->jmid_is_connected )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] NO isconnected method of %s ! [FILE:%s, LINE:%d]", JAVA_HIVE_CLASS_NAME.c_str(), __FILE__, __LINE__);
	}

	// 获取 Java HIVE 代理类的 GetErrorMsg 方法
	m_pJNI->jmid_get_error_msg = m_pJNI->p_jni_env->GetMethodID(jclass_hiveagent, "GetErrorMsg", "()Ljava/lang/String;");
	if ( NULL == m_pJNI->jmid_get_error_msg )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] NO geterrormsg method of %s ! [FILE:%s, LINE:%d]", JAVA_HIVE_CLASS_NAME.c_str(), __FILE__, __LINE__);
	}

	// 获取 Java HIVE 代理类的 ExecuteSQL 方法
	m_pJNI->jmid_execute_sql = m_pJNI->p_jni_env->GetMethodID(jclass_hiveagent, "ExecuteSQL", "(Ljava/lang/String;)Z");
	if ( NULL == m_pJNI->jmid_execute_sql )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] NO executesql method of %s ! [FILE:%s, LINE:%d]", JAVA_HIVE_CLASS_NAME.c_str(), __FILE__, __LINE__);
	}

	// 获取 Java HIVE 代理类的 FetchData 方法
	m_pJNI->jmid_fetch_data = m_pJNI->p_jni_env->GetMethodID(jclass_hiveagent, "FetchData", "(Ljava/lang/String;)Ljava/util/ArrayList;");
	if ( NULL == m_pJNI->jmid_fetch_data )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] NO fetchdata method of %s ! [FILE:%s, LINE:%d]", JAVA_HIVE_CLASS_NAME.c_str(), __FILE__, __LINE__);
	}

	// 找寻 Java ArrayList 类
	jclass jclass_array = m_pJNI->p_jni_env->FindClass("java/util/ArrayList");
	if ( NULL == jclass_array )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] Can not find Java class: java/util/ArrayList [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 获取 Java ArrayList 类的 size 方法
	m_pJNI->jmid_array_size = m_pJNI->p_jni_env->GetMethodID(jclass_array, "size", "()I");
	if ( NULL == m_pJNI->jmid_array_size )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] NO size method of ArrayList ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 获取 Java ArrayList 类的 get 方法
	m_pJNI->jmid_array_get = m_pJNI->p_jni_env->GetMethodID(jclass_array, "get", "(I)Ljava/lang/Object;");
	if ( NULL == m_pJNI->jmid_array_get )
	{
		throw Exception(BJH_INIT_FAILED, "[BASE HIVE] NO get method of ArrayList ! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[BASE HIVE] Init %s OK.", JAVA_HIVE_CLASS_NAME.c_str());
}

}	// namespace base

