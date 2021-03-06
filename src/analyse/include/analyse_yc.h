#pragma once

#include "analyse.h"
#include "ycinfo.h"

class YCAnaDB2;
class YCStatFactor;

// 业财稽核-分析模块
class Analyse_YC : public Analyse
{
public:
	Analyse_YC();
	virtual ~Analyse_YC();

public:
	// 载入参数配置信息
	virtual void LoadConfig();

	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();

	// 初始化
	virtual void Init();

	// 任务结束（资源回收）
	virtual void End(int err_code, const std::string& err_msg = std::string());

protected:
	// 创建新的数据库连接
	virtual CAnaDB2* CreateDBConnection();

	// 获取后续参数任务信息
	virtual void GetExtendParaTaskInfo(VEC_STRING& vec_str);

	// 解析分析规则，生成Hive取数逻辑
	virtual void AnalyseRules(VEC_STRING& vec_hivesql);

	// Delete the SQL function: sum
	void RemoveSumFunc(VEC_STRING& vec_hivesql);

	// 检查：是否为业财稽核类型。输出中文描述
	bool CheckYCAnalyseType(std::string& cn_type) const;

	// 生成数据库信息
	virtual void GetAnaDBInfo();

	// 释放稽核统计因子资源
	void ReleaseStatFactor();

	// 获取任务信息
	virtual void FetchTaskInfo();

	// 采集时间转换
	virtual void EtlTimeConvertion();

	// 取得详情表-业务侧更新字段列表
	void FetchUpdateFields_YW();

	// 从指标字段集中获取更新字段
	void FetchUpdateFieldsFromKpiCol(std::vector<KpiColumn>& vec_kc, VEC_STRING& vec_up_fd);

	// 创建稽核统计因子
	void CreateStatFactor();

	// 获取因子项目和值个数
	void GetFactorItemValSize(int& item_size, int& val_size) const;

	// 计算指标字段集的项目和值个数
	void CountKpiColumnItemValSize(const std::vector<KpiColumn>& vec_column, int& item_size, int& val_size) const;

	// 分析业财稽核源数据，生成结果数据
	virtual void AnalyseSourceData();

	// 生成最新的批次
	void GenerateNewBatch();

	// 设置核对表稽核的最新批次
	void SetNewBatch_HDB();

	// 设置详情表稽核的最新批次
	void SetNewBatch_XQB();

	// 统计因子转换
	void ConvertStatFactor();

	// 生成稽核结果数据
	void GenerateResultData();

	// 结果数据入库 [DB2]
	virtual void StoreResult();

	// 入库报表稽核统计结果
	void StoreReportResult();

	// 入库详情表（财务侧）结果
	void StoreDetailResult_CW();

	// 入库差异汇总结果
	void StoreDiffSummaryResult();

	// 保留上一批次的手工填列的数据
	void KeepLastBatchManualData();

	// 登记信息
	void RecordInformation();

	// 登记稽核记录日志
	void RecordStatisticsLog();

	// 获取详情表地市信息
	std::string GetCity_XQB();

	// 获取报表状态-类型
	std::string GetReportStateType();

	// 登记报表状态
	// "00"-待审核，"07"-正在稽核，"08"-稽核失败
	void RecordReportState(const std::string& state);

	// 登记流程记录日志
	void RecordProcessLog();

	// 删除采集目标表
	void DropEtlTargetTable();

	// 更新维度取值范围
	virtual void UpdateDimValue();

protected:
	YCAnaDB2*   m_pYCDB2;					// 业财数据库接口（DB2）

protected:
	std::string m_tabYCTaskReq;				// 任务请求表
	std::string m_tabStatRule;				// 统计因子规则表
	std::string m_tabStatLog;				// 稽核记录日志表
	std::string m_tabReportStat;			// 报表状态表
	std::string m_tabProcessLog;			// 流程记录日志表
	std::string m_fieldPeriod;				// 账期字段名
	std::string m_fieldCity;				// 地市字段名
	std::string m_fieldBatch;				// 批次字段名

protected:
	UpdateFields_YW m_updFields;			// 详情表（业务侧）更新字段
	YCReportState   m_reportState;			// 报表状态

protected:
	YCTaskReq     m_taskReq;				// 任务请求信息
	YCStatFactor* m_pStatFactor;			// 稽核统计因子
	VEC2_STRING   m_v2DiffSummary;			// 差异汇总数据
};

