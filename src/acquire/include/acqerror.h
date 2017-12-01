#pragma once

enum ACQUIRE_ERROR
{
	// 采集模块错误码
	ACQERR_TASKINFO_ERROR          = -2000001,			// 任务信息异常
	ACQERR_KPIID_INVALID           = -2000002,			// 指标ID无效
	ACQERR_ETLID_INVALID           = -2000003,			// 采集规则ID无效
	ACQERR_HIVE_PORT_INVALID       = -2000004,			// Hive服务器端口无效
	ACQERR_INIT_FAILED             = -2000005,			// 初始化失败
	ACQERR_TASKINFO_INVALID        = -2000006,			// 任务信息无效
	ACQERR_TRANS_DATASRC_FAILED    = -2000007,			// 源表转换失败
	ACQERR_OUTER_JOIN_FAILED       = -2000008,			// 外连条件下生成Hive SQL失败
	ACQERR_DATA_ACQ_FAILED         = -2000009,			// 数据采集失败
	ACQERR_HDFS_PORT_INVALID       = -2000010,			// HDFS端口无效
	ACQERR_OUTPUT_HDFS_FILE_FAILED = -2000011,			// 输出到HDFS文件失败
	ACQERR_LOAD_HIVE_FAILED        = -2000012,			// 载入数据到HIVE失败
	ACQERR_CHECK_SRC_TAB_FAILED    = -2000013,			// 检查源表失败
	ACQERR_GEN_ETL_DATE_FAILED     = -2000014,			// 生成采集时间失败
	ACQERR_EXCHANGE_SQLMARK_FAILED = -2000015,			// 标记转换失败
	ACQERR_HANDLE_YCINFO_FAILED    = -2000016,			// 处理业财稽核因子规则失败
	ACQERR_SQL_EXTEND_CONV_FAILED  = -2000017,			// SQL 扩展转换失败
	ACQERR_CHECK_OUTER_TAB_FAILED  = -2000018,			// 检查外连表失败
	ACQERR_CREATE_SQLEXTENDCONV    = -2000019,			// 创建 SQL 扩展转换失败

	// HIVE 类错误码
	ACQERR_REBUILD_TABLE           = -2001001,			// 重建Hive表失败
	ACQERR_EXECUTE_ACQSQL          = -2001002,			// 执行采集SQL失败
	ACQERR_CHECK_TAB_EXISTED       = -2001003,			// 检查表是否存在失败

	// 数据库类错误码
	ACQERR_SEL_ETL_RULE            = -2002001,			// 查询采集规则出错
	ACQERR_SEL_ETL_DIM             = -2002002,			// 查询采集维度规则出错
	ACQERR_SEL_ETL_VAL             = -2002003,			// 查询采集值规则出错
	ACQERR_FETCH_ETL_DATA          = -2002004,			// 执行数据采集出错
	ACQERR_CHECK_SRC_TAB           = -2002005,			// 检查表是否存在出错
	ACQERR_SEL_ETL_SRC             = -2002006,			// 查询采集数据源出错
	ACQERR_SEL_YC_STATRULE         = -2002007,			// 查询业财稽核因子规则信息出错
	ACQERR_SEL_YCTASKREQ           = -2002008,			// 查询任务请求表的信息出错
	ACQERR_UPD_YC_TASK_REQ         = -2002009,			// （业财）更新任务请求表出错
	ACQERR_UPD_TSLOG_STATE         = -2002010,			// 更新任务日程日志表状态出错
	ACQERR_SEL_YCTASKCITY_CN       = -2002011,			// 查询任务地市的中文名称出错
	ACQERR_UPD_INS_REPORTSTATE     = -2002012,			// 更新或插入报表状态表出错
	ACQERR_SEL_KPI_RULE_TYPE       = -2002013,			// 查询指标规则类型出错

	// 采集任务信息的错误码
	ACQERR_TRANS_VAL_SRC_NAME      = -2003001,			// 采集值对应源字段名转换失败

	// HDFS 连接类错误码
	ACQERR_HDFS_CONNECT_FAILED	   = -2004001,			// 连接失败
};

