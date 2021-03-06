#pragma once

#include "analyse.h"

// 序列
struct SeqNode
{
public:
	SeqNode(): index(-1)
	{}

public:
	int                      index;					// 序列位置索引
	std::string              seq_name;				// 序列名
	std::vector<std::string> vec_seq_val;			// 序列的值队列
};

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
	// 获取后续参数任务信息
	virtual void GetExtendParaTaskInfo(VEC_STRING& vec_str);

	// 解析分析规则，生成Hive取数逻辑
	virtual void AnalyseRules(VEC_STRING& vec_hivesql);

	// 从分析表达式中生成Hive取数逻辑
	void GetExpressHiveSQL(VEC_STRING& vec_hivesql);

	// 从HIVE SQL中提取数据库序列Sequence
	// 若HIVE SQL非法则返回false，否则无论是否成功提取到序列都会返回true
	bool GetSequenceInHiveSQL(std::string& hive_sql);

	// 生成数据删除的时间(段)
	void GenerateDeleteTime();

	// 生成执行SQL队列
	void GetExecuteSQL();

	// 按类型生成目标表名称
	virtual void GenerateTableNameByType();

	// 数据补全
	virtual void DataSupplement();

	// 结果数据入库 [DB2]
	virtual void StoreResult();

	// 删除旧数据
	virtual void RemoveOldResult(const AnaTaskInfo::ResultTableType& result_tabtype);

	// 更新维度取值范围
	virtual void UpdateDimValue();

protected:
	int m_begintime;				// 开始时间（包含）
	int m_endtime;					// 结束时间（包含）

protected:
	std::vector<SeqNode> m_vecSeq;
	VEC_STRING           m_vecExecSql;			// 执行SQL队列
};

