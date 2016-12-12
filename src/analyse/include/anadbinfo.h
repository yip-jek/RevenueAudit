#pragma once

#include <string>
#include <vector>
#include "pubtime.h"

// 时间字段
struct TimeField
{
public:
	static const int TF_INVALID_INDEX = -1;

public:
	TimeField(): valid(false), index(TF_INVALID_INDEX)
	{}

public:
	bool        valid;					// 是否有效
	int         index;					// 所在位置索引
	std::string str_time;
};

struct AnaField
{
	std::string field_name;				// 字段名
	std::string CN_name;				// 中文名
};

// 数据库信息 [DB2]
class AnaDBInfo
{
public:
	AnaDBInfo();
	~AnaDBInfo();

public:
	// 生成采集时间
	bool GenerateEtlDay(const std::string& etl_time, int index);

	// 生成当前时间
	void GenerateNowDay(bool is_valid, int index);

	// 获取采集时间类型
	base::PubTime::DATE_TYPE GetEtlDateType() const;

	// 采集时间是否有效
	bool IsEtlDayValid() const;

	// 当前时间是否有效
	bool IsNowDayValid() const;

	// 获取采集时间所在位置索引
	int GetEtlDayIndex() const;

	// 获取当前时间所在位置索引
	int GetNowDayIndex() const;

	// 获取采集时间
	std::string GetEtlDay() const;

	// 获取当前时间
	std::string GetNowDay() const;

	// 设置目标表字段信息
	void SetAnaFields(std::vector<AnaField>& v_fields);

	// 获取目标字段数
	int GetFieldSize() const;

	// 获取目标字段信息
	AnaField GetAnaField(int index) const;

	// 获取采集时间字段名
	std::string GetEtlDayFieldName() const;

public:
	std::string              target_table;				// 最终目标表名
	std::string              backup_table;				// 目标表的备份表（仅用于报表统计）
	std::string              db2_sql;					// 数据库SQL语句

private:
	base::PubTime::DATE_TYPE date_type;					// 采集时间类型
	TimeField                tf_etlday;					// 采集时间字段
	TimeField                tf_nowday;					// 当前时间字段（格式：YYYYMMDD）
	std::vector<AnaField>    vec_fields;				// 目标表字段信息
};

