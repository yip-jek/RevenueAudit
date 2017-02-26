#pragma once

#include "acqtaskinfo.h"
#include "exception.h"

// 采集任务信息的工具类
class TaskInfoUtil
{
public:
	enum TASK_INFO_UTIL_ERROR
	{
		TERR_TRANS_VAL_SRC_NAME = -2003001,			// 采集值对应源字段名转换失败
	};

	static const char* const S_SRC_VAL_RECORD;				// 源表的值：记录数
	static const char* const S_SRC_VAL_NEGATIVE_SUM;		// 源表的值：负的和

public:
	// 获取目标表的维度SQL
	static std::string GetTargetDimSql(AcqEtlDim& target_dim);

	// 获取目标表的值SQL
	static std::string GetTargetValSql(AcqEtlVal& target_val);

	// 获取采集规则的维度SQL
	static std::string GetEtlDimSql(AcqEtlDim& etl_dim, bool set_as, const std::string& tab_prefix = std::string());

	// 检查：是否为指定的特殊采集值
	// 格式：特殊标识(值)
	static bool CheckSpecialVal(const std::string& spec, const std::string& val_src, std::string& val);

	// 在值字段名前加上前缀
	static std::string AddPrefixVal(const std::string& val_sql, const std::string& prefix);

	// 采集值对应源字段名转换
	static std::string TransEtlValSrcName(OneEtlVal& val, const std::string& tab_prefix = std::string()) throw(base::Exception);

	// 获取采集规则的值SQL
	static std::string GetEtlValSql(AcqEtlVal& etl_val, const std::string& tab_prefix = std::string());

	//static std::string TrimUpperDimMemo(OneEtlDim& dim);
	//static std::string TrimUpperValMemo(OneEtlVal& val);

	//// 是否为外连关联维度字段
	//static bool IsOuterJoinOnDim(OneEtlDim& dim);

	//// 是否为外连表的维度字段
	//static bool IsOuterTabJoinDim(OneEtlDim& dim);

	//// 是否为外连表的值字段
	//static bool IsOuterTabJoinVal(OneEtlVal& val);

	// 获取采集维度规则的(外连)关联字段个数
	static int GetNumOfEtlDimJoinOn(AcqEtlDim& etl_dim);

	// 获取外连条件下采集规则的取数SQL
	static std::string GetOuterJoinEtlSQL(AcqEtlDim& etl_dim, AcqEtlVal& etl_val, const std::string& src_tab, const std::string& outer_tab, std::vector<std::string>& vec_join_on, const std::string& cond);

	// 获得表名：支持直接表名，或从查询SQL语句中获取表名
	static void GetTableNames(const std::string& src, std::vector<std::string>& vec_tabname);
};

