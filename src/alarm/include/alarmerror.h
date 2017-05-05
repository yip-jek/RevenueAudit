#pragma once

// 告警模块：（全局）错误码
enum ALARM_ERROR
{
	ALMERR_LOAD_BASIC_CFG = -4000001,				// 载入基础配置失败
	ALMERR_ALARMMGR_RUN   = -4000002,				// 告警管理：任务执行出错
};

