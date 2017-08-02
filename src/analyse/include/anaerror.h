#pragma once

// 分析程序：（全局）错误码
enum ANALYSE_ERROR
{
	// 分析模块错误码
	ANAERR_TASKINFO_ERROR         = -3000001,			// 任务信息异常
	ANAERR_KPIID_INVALID          = -3000002,			// 指标ID无效
	ANAERR_ANAID_INVALID          = -3000003,			// 分析规则ID无效
	ANAERR_HIVE_PORT_INVALID      = -3000004,			// Hive服务器端口无效
	ANAERR_INIT_FAILED            = -3000005,			// 初始化失败
	ANAERR_TASKINFO_INVALID       = -3000006,			// 任务信息无效
	ANAERR_ANA_RULE_FAILED        = -3000007,			// 解析分析规则失败
	ANAERR_GENERATE_TAB_FAILED    = -3000008,			// 生成目标表名失败
	ANAERR_GET_DBINFO_FAILED      = -3000009,			// 获取数据库信息失败
	ANAERR_GET_SUMMARY_FAILED     = -3000010,			// 生成汇总对比HIVE SQL失败
	ANAERR_GET_DETAIL_FAILED      = -3000011,			// 生成明细对比HIVE SQL失败
	ANAERR_DETERMINE_GROUP_FAILED = -3000012,			// 确定数据组失败
	ANAERR_SPLIT_HIVESQL_FAILED   = -3000013,			// 拆分可执行HIVE SQL失败
	ANAERR_GET_STATISTICS_FAILED  = -3000014,			// 生成一般统计HIVE SQL失败
	ANAERR_GET_REPORT_STAT_FAILED = -3000015,			// 生成报表统计HIVE SQL失败
	ANAERR_GET_STAT_BY_SET_FAILED = -3000016,			// 生成指定组的统计HIVE SQL失败
	ANAERR_COMPARE_RESULT_DATA    = -3000017,			// 生成对比结果失败
	ANAERR_SRC_DATA_UNIFIED_CODE  = -3000018,			// 源数据的统一编码转换失败
	ANAERR_TRANS_YCFACTOR_FAILED  = -3000019,			// 业财稽核统计因子转换失败
	ANAERR_GENERATE_YCDATA_FAILED = -3000020,			// 生成业财稽核数据失败
	ANAERR_GET_EXP_HIVESQL_FAILED = -3000021,			// 从分析表达式中生成HIVE SQL失败
	ANAERR_EXCHG_SQLMARK_FAILED   = -3000022,			// 标志转换失败
	ANAERR_GENE_DELTIME_FAILED    = -3000023,			// 生成数据删除的时间失败
	ANAERR_STORE_RESULT_FAILED    = -3000024,			// 结果数据入库失败
	ANAERR_SET_NEWBATCH_XQB       = -3000025,			// 设置详情表稽核的最新批次失败

	// HIVE类错误码
	ANAERR_FETCH_SRCDATA_FAILED   = -3001001,			// 获取源数据失败
	ANAERR_EXECUTE_SQL_FAILED     = -3001002,			// 执行HIVE SQL失败

	// 数据库类错误码
	ANAERR_SEL_KPI_RULE           = -3002001,			// 查询指标规则出错
	ANAERR_SEL_KPI_COL            = -3002002,			// 查询指标字段出错
	ANAERR_SEL_DIM_VALUE          = -3002003,			// 查询维度取值出错
	ANAERR_INS_DIM_VALUE          = -3002004,			// 插入维度取值出错
	ANAERR_SEL_ETL_RULE           = -3002005,			// 查询采集规则出错
	ANAERR_SEL_ANA_RULE           = -3002006,			// 查询分析规则出错
	ANAERR_SEL_ETL_DIM            = -3002007,			// 查询采集维度规则出错
	ANAERR_SEL_ETL_VAL            = -3002008,			// 查询采集值规则出错
	ANAERR_SEL_REPORT_DATA        = -3002009,			// 统计报表统计数据出错
	ANAERR_SEL_CHANN_UNICODE      = -3002010,			// 获取渠道统一编码数据出错
	ANAERR_SEL_CITY_UNICODE       = -3002011,			// 获取地市统一编码数据出错
	ANAERR_SEL_ALL_CITY           = -3002012,			// 获取全量地市（唯一）出错
	ANAERR_SEL_MAX_EVENTID        = -3002013,			// 获取最大告警事件 ID 出错
	ANAERR_SEL_COM_RES_DESC       = -3002014,			// 查询对比结果描述出错
	ANAERR_ALTER_EMPTY_TAB        = -3002015,			// 清空结果表数据出错
	ANAERR_DEL_FROM_TAB           = -3002016,			// 删除结果表数据出错
	ANAERR_INS_RESULT_DATA        = -3002017,			// 插入结果数据出错
	ANAERR_SEL_YC_STATRULE        = -3002018,			// 查询业财稽核因子规则信息出错
	ANAERR_UPD_YC_TASK_REQ        = -3002019,			// 更新任务请求表出错
	ANAERR_SEL_SEQUENCE           = -3002020,			// 获取数据库序列出错
	ANAERR_SEL_YC_TASK_CITY       = -3002021,			// 获取任务请求的地市信息出错
	ANAERR_SEL_HDB_MAX_BATCH      = -3002022,			// 获取地市核对表的最新批次出错
	ANAERR_SEL_XQB_MAX_BATCH      = -3002023,			// 获取地市详情表的最新批次出错
	ANAERR_INS_YC_STAT_LOG        = -3002024,			// 入库业财稽核记录日志出错
	ANAERR_SEL_SRC_MAX_BATCH      = -3002025,			// 获取业财数据源表最新批次出错
	ANAERR_UPD_TSLOG_STATE        = -3002026,			// 更新任务日程日志表状态出错
	ANAERR_EXECUTE_SQL            = -3002027,			// 直接执行 SQL 出错
	ANAERR_UPD_INS_DIFFSUMMARY    = -3002028,			// (业财) 更新或插入差异汇总结果数据出错
	ANAERR_UPD_INS_REPORTSTATE    = -3002029,			// (业财) 更新或插入报表状态表的状态出错
	ANAERR_UPD_INS_PROCESSLOG     = -3002030,			// (业财) 更新或插入流程记录日志表的状态出错

	// 编码转换错误码
	ANAERR_INPUT_CHANN_UNICODE    = -3003001,			// 录入渠道统一编码信息失败
	ANAERR_INPUT_CITY_UNICODE     = -3003002,			// 录入地市统一编码信息失败

	// 对比数据类错误码
	ANAERR_SET_COMPARE_DATA       = -3004001,			// 设置对比数据失败
	ANAERR_GET_COMPARE_RESULT     = -3004002,			// 获取对比结果数据失败

	// （业财）规则因子类错误码
	ANAERR_LOAD_STAT_INFO         = -3005001,			// 载入规则因子信息失败
	ANAERR_LOAD_FACTOR            = -3005002,			// 载入因子对失败
	ANAERR_MAKE_STATINFO_RESULT   = -3005003,			// 由因子规则生成结果数据失败
	ANAERR_CALC_COMPLEX_FACTOR    = -3005004,			// 计算业财稽核组合因子失败
	ANAERR_OPERATE_ONE_FACTOR     = -3005005,			// 计算单个因子失败
	ANAERR_CALC_CATEGORY_FACTOR   = -3005006,			// 计算组合分类因子的维度值失败
	ANAERR_EXPAND_CATEGORY_INFO   = -3005007,			// 扩展分类因子信息失败
	ANAERR_MATCH_CATEGORY_FACTOR  = -3005008,			// 匹配一般分类因子失败
	ANAERR_STORE_DIFF_SUMMARY     = -3005009,			// 入库差异汇总结果数据失败
};

