#pragma once

// 告警模块：（全局）错误码
enum ALARM_ERROR
{
	// 基础告警管理模块
	ALMERR_LOAD_BASIC_CFG = -4000001,				// 载入基础配置失败
	ALMERR_ALARMMGR_RUN   = -4000002,				// 告警管理：任务执行出错

	// 一点稽核告警管理模块
	ALMERR_INIT_DB_CONN   = -4001001,				// 初始化数据库连接出错

	// 一点稽核数据库模块
	ALMERR_SEL_ALARM_REQ  = -4002001,				// 获取告警请求记录出错
	ALMERR_UPD_ALARM_REQ  = -4002002,				// 更新告警请求记录出错
};

