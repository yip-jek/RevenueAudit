#pragma once

#include "analyse.h"

// 话单稽核-分析模块
class Analyse_HD : public Analyse
{
public:
	Analyse_HD();
	virtual ~Analyse_HD();

public:
	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();

protected:
	// 解析分析规则，生成Hive取数逻辑
	virtual void AnalyseRules(std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 从分析表达式中生成Hive取数逻辑
	void GetExpressHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception);

	// 从HIVE SQL中提取数据库序列Sequence
	// 若HIVE SQL非法则返回false，否则无论是否成功提取到序列都会返回true
	bool GetSequenceInHiveSQL(std::string& hive_sql);

	// 生成数据删除的时间(段)
	void GenerateDeleteTime() throw(base::Exception);

	// 生成执行SQL队列
	void GetExecuteSQL();

	// 按类型生成目标表名称
	virtual void GenerateTableNameByType() throw(base::Exception);

	// 数据补全
	virtual void DataSupplement();

	// 结果数据入库 [DB2]
	virtual void StoreResult() throw(base::Exception);

	// 删除旧数据
	virtual void RemoveOldResult(const AnaTaskInfo::ResultTableType& result_tabtype) throw(base::Exception);

	// 告警判断: 如果达到告警阀值，则生成告警
	virtual void AlarmJudgement() throw(base::Exception);

	// 更新维度取值范围
	virtual void UpdateDimValue();

protected:
	int m_begintime;				// 开始时间（包含）
	int m_endtime;					// 结束时间（包含）

protected:
	std::vector<std::string> m_vecExecSql;				// 执行SQL队列
};

