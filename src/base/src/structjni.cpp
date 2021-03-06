#include "structjni.h"

namespace base
{

StructJNI::StructJNI()
:p_jvm(NULL)
,p_jni_env(NULL)
,jobj_hiveagent(NULL)
,jmid_set_zkquorum(NULL)
,jmid_set_krb5conf(NULL)
,jmid_set_usrkeytab(NULL)
,jmid_set_principal(NULL)
,jmid_set_jaasconf(NULL)
,jmid_init(NULL)
,jmid_connect(NULL)
,jmid_disconnect(NULL)
,jmid_is_connected(NULL)
,jmid_get_error_msg(NULL)
,jmid_execute_sql(NULL)
,jmid_fetch_data(NULL)
,jmid_array_size(NULL)
,jmid_array_get(NULL)
{
}

std::string StructJNI::JString2String(jstring& j_str)
{
	std::string str;
	if ( j_str != NULL )
	{
		const char* p_cstr = p_jni_env->GetStringUTFChars(j_str, NULL);
		str = p_cstr;
		p_jni_env->ReleaseStringUTFChars(j_str, p_cstr);
	}

	return str;
}

}	// namespace base

