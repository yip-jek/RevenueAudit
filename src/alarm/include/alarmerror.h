#pragma once

// 告警模块：（全局）错误码
enum ALARM_ERROR
{
	// 基础告警管理模块
	ALMERR_LOAD_BASIC_CFG     = -4000001,				// 载入基础配置失败
	ALMERR_INIT               = -4000002,				// 初始化出错
	ALMERR_ALARMMGR_RUN       = -4000003,				// 告警管理：任务执行出错

	// 一点稽核告警管理模块
	ALMERR_INIT_DB_CONN       = -4001001,				// 初始化数据库连接出错
	ALMERR_INIT_ALARM_FILE    = -4001002,				// 初始化告警（短信）文件出错

	// 一点稽核数据库模块
	ALMERR_SEL_ALARM_REQ      = -4002001,				// 获取告警请求记录出错
	ALMERR_UPD_ALARM_REQ      = -4002002,				// 更新告警请求记录出错
	ALMERR_UPD_ALARM_THRES    = -4002003,				// 获取告警阈值记录出错
	ALMERR_SEL_ALARM_SRCDAT   = -4002004,				// 采集告警源数据出错
	ALMERR_INS_ALARM_INFO     = -4002005,				// 存储告警信息出错

	// 一点稽核告警（短信）文件模块
	ALMERR_AFILE_SET_PATH     = -4003001,				// 设置文件路径出错
	ALMERR_AFILE_SET_FILE_FMT = -4003002,				// 设置文件名格式出错
	ALMERR_AFILE_SET_MAX_LINE = -4003003,				// 设置文件最大行数出错
};

