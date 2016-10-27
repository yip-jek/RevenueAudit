#pragma once

#include <string>
#include <vector>
#include <map>
#include "pubstr.h"

// 单个维度信息
struct OneEtlDim
{
public:
	// 维度备注类型
	enum DIM_MEMO_TYPE
	{
		DMTYPE_NONE			= 0,			// 无（即没有指定类型）
		DMTYPE_JOIN_ON		= 1,			// 外连-关联类型
		DMTYPE_JOIN_DIM		= 2,			// 外连-维度类型
		DMTYPE_SHOW_UP		= 3,			// 显示类型（用于单独显示的维度）
	};

public:
	OneEtlDim(): EtlDimSeq(0), EtlDimMemo(DMTYPE_NONE)
	{}

	OneEtlDim(const OneEtlDim& dim)
	:EtlDimID(dim.EtlDimID)
	,EtlDimSeq(dim.EtlDimSeq)
	,EtlDimName(dim.EtlDimName)
	,EtlDimDesc(dim.EtlDimDesc)
	,EtlDimSrcName(dim.EtlDimSrcName)
	,EtlDimMemo(dim.EtlDimMemo)
	{}

public:
	const OneEtlDim& operator = (const OneEtlDim& dim)
	{
		if ( this != &dim )
		{
			this->EtlDimID      = dim.EtlDimID;
			this->EtlDimSeq     = dim.EtlDimSeq;
			this->EtlDimName    = dim.EtlDimName;
			this->EtlDimDesc    = dim.EtlDimDesc;
			this->EtlDimSrcName = dim.EtlDimSrcName;
			this->EtlDimMemo    = dim.EtlDimMemo;
		}

		return *this;
	}

	void Clear()
	{
		EtlDimSeq = 0;

		EtlDimID.clear();
		EtlDimName.clear();
		EtlDimDesc.clear();
		EtlDimSrcName.clear();
		EtlDimMemo = DMTYPE_NONE;
	}

public:
	// 设置维度备注类型
	void SetDimMemoType(const std::string& m_type)
	{
		const std::string TYPE = base::PubStr::TrimUpperB(m_type);

		if ( "JOIN_ON" == TYPE )		// 外连-关联类型
		{
			EtlDimMemo = DMTYPE_JOIN_ON;
		}
		else if ( "JOIN_DIM" == TYPE )	// 外连-维度类型
		{
			EtlDimMemo = DMTYPE_JOIN_DIM;
		}
		else if ( "SHOW_UP" == TYPE )	// 显示类型
		{
			EtlDimMemo = DMTYPE_SHOW_UP;
		}
		else	// 无
		{
			EtlDimMemo = DMTYPE_NONE;
		}
	}

	// 维度备注类型的字符串表示
	std::string GetDimMemoTypeStr() const
	{
		switch ( EtlDimMemo )
		{
		case DMTYPE_NONE:
			return std::string("<NONE>");
		case DMTYPE_JOIN_ON:
			return std::string("JOIN_ON");
		case DMTYPE_JOIN_DIM:
			return std::string("JOIN_DIM");
		case DMTYPE_SHOW_UP:
			return std::string("SHOW_UP");
		default:
			return std::string("<-Unknown->");
		}
	}

public:
	std::string		EtlDimID;					// 采集维度ID
	int		   		EtlDimSeq;					// 采集维度序号
	std::string		EtlDimName;					// 采集维度名称
	std::string		EtlDimDesc;					// 采集维度描述
	std::string		EtlDimSrcName;				// 维度对应源字段名称
	DIM_MEMO_TYPE	EtlDimMemo;					// 维度备注类型
};

// 采集维度信息
struct AcqEtlDim
{
public:
	AcqEtlDim(): isValid(false)
	{}

	AcqEtlDim(const AcqEtlDim& dim)
	:isValid(dim.isValid)
	,acqEtlDimID(dim.acqEtlDimID)
	,vecEtlDim(dim.vecEtlDim)
	{
		//this->vecEtlDim.insert(this->vecEtlDim.begin(), dim.vecEtlDim.begin(), dim.vecEtlDim.end());
	}

public:
	const AcqEtlDim& operator = (const AcqEtlDim& dim)
	{
		if ( this != &dim )
		{
			this->isValid     = dim.isValid;
			this->acqEtlDimID = dim.acqEtlDimID;

			//this->vecEtlDim.insert(this->vecEtlDim.begin(), dim.vecEtlDim.begin(), dim.vecEtlDim.end());
			this->vecEtlDim   = dim.vecEtlDim;
		}

		return *this;
	}

	void Clear()
	{
		isValid = false;
		acqEtlDimID.clear();

		std::vector<OneEtlDim>().swap(vecEtlDim);
	}

public:
	bool					isValid;			// 是否有效
	std::string				acqEtlDimID;		// 采集维度ID
	std::vector<OneEtlDim>	vecEtlDim;			// 维度信息集
};

// 单个采集值信息
struct OneEtlVal
{
public:
	// 值备注类型
	enum VAL_MEMO_TYPE
	{
		VMTYPE_NONE		= 0,			// 无（即没有指定类型）
		VMTYPE_JOIN_VAL	= 1,			// 外连-值类型
		//VMTYPE_SHOW_UP	= 2,			// 显示类型（用于单独显示的值）
	};

public:
	OneEtlVal(): EtlValSeq(0), EtlValMemo(VMTYPE_NONE)
	{}

	OneEtlVal(const OneEtlVal& val)
	:EtlValID(val.EtlValID)
	,EtlValSeq(val.EtlValSeq)
	,EtlValName(val.EtlValName)
	,EtlValDesc(val.EtlValDesc)
	,EtlValSrcName(val.EtlValSrcName)
	,EtlValMemo(val.EtlValMemo)
	{}

public:
	const OneEtlVal& operator = (const OneEtlVal& val)
	{
		if ( this != &val )
		{
			this->EtlValID      = val.EtlValID;
			this->EtlValSeq     = val.EtlValSeq;
			this->EtlValName    = val.EtlValName;
			this->EtlValDesc    = val.EtlValDesc;
			this->EtlValSrcName = val.EtlValSrcName;
			this->EtlValMemo    = val.EtlValMemo;
		}

		return *this;
	}

	void Clear()
	{
		EtlValSeq = 0;

		EtlValID.clear();
		EtlValName.clear();
		EtlValDesc.clear();
		EtlValSrcName.clear();
		EtlValMemo = VMTYPE_NONE;
	}

public:
	// 设置值备注类型
	void SetValMemoType(const std::string& m_type)
	{
		const std::string TYPE = base::PubStr::TrimUpperB(m_type);

		if ( "JOIN_VAL" == TYPE )		// 外连-值类型
		{
			EtlValMemo = VMTYPE_JOIN_VAL;
		}
		//else if ( "SHOW_UP" == TYPE )	// 显示类型
		//{
		//	EtlValMemo = VMTYPE_SHOW_UP;
		//}
		else	// 无
		{
			EtlValMemo = VMTYPE_NONE;
		}
	}

	// 值备注类型的字符串表示
	std::string GetValMemoTypeStr() const
	{
		switch ( EtlValMemo )
		{
		case VMTYPE_NONE:
			return std::string("<NONE>");
		case VMTYPE_JOIN_VAL:
			return std::string("JOIN_VAL");
		//case VMTYPE_SHOW_UP:
		//	return std::string("SHOW_UP");
		default:
			return std::string("<-Unknown->");
		}
	}

public:
	std::string		EtlValID;					// 采集值ID
	int				EtlValSeq;					// 采集值序号
	std::string		EtlValName;					// 采集值名称
	std::string		EtlValDesc;					// 采集值描述
	std::string		EtlValSrcName;				// 值对应源字段名称
	VAL_MEMO_TYPE	EtlValMemo;					// 值备注类型
};

// 采集值信息
struct AcqEtlVal
{
public:
	AcqEtlVal(): isValid(false)
	{}

	AcqEtlVal(const AcqEtlVal& val)
	:isValid(val.isValid)
	,acqEtlValID(val.acqEtlValID)
	,vecEtlVal(val.vecEtlVal)
	{
		//this->vecEtlVal.insert(this->vecEtlVal.begin(), val.vecEtlVal.begin(), val.vecEtlVal.end());
	}

public:
	const AcqEtlVal& operator = (const AcqEtlVal& val)
	{
		if ( this != &val )
		{
			this->isValid     = val.isValid;
			this->acqEtlValID = val.acqEtlValID;

			//this->vecEtlVal.insert(this->vecEtlVal.begin(), val.vecEtlVal.begin(), val.vecEtlVal.end());
			this->vecEtlVal   = val.vecEtlVal;
		}

		return *this;
	}

	void Clear()
	{
		isValid = false;
		acqEtlValID.clear();

		std::vector<OneEtlVal>().swap(vecEtlVal);
	}

public:
	bool					isValid;			// 是否有效
	std::string				acqEtlValID;		// 采集值ID
	std::vector<OneEtlVal>	vecEtlVal;			// 值信息集
};

// 采集数据源信息
struct EtlSrcInfo
{
public:
	// 数据源条件类型
	enum EtlSrcType
	{
		ESTYPE_UNKNOWN		= 0,		// 未知类型
		ESTYPE_STRAIGHT		= 1,		// 直接条件
	};

public:
	EtlSrcInfo(): src_type(ESTYPE_UNKNOWN)
	{}
	EtlSrcInfo(const EtlSrcInfo& es_info)
	:src_type(es_info.src_type)
	,condition(es_info.condition)
	{}

	const EtlSrcInfo& operator = (const EtlSrcInfo& es_info)
	{
		if ( this != &es_info )
		{
			this->src_type  = es_info.src_type;
			this->condition = es_info.condition;
		}

		return *this;
	}

public:
	// 设置数据源条件类型
	bool SetEtlSrcType(const std::string& type)
	{
		const std::string C_TYPE = base::PubStr::TrimUpperB(type);

		if ( "STRAIGHT" == C_TYPE )
		{
			src_type = ESTYPE_STRAIGHT;
		}
		else
		{
			src_type = ESTYPE_UNKNOWN;
			return false;
		}

		return true;
	}

public:
	EtlSrcType		src_type;			// 数据源条件类型
	std::string		condition;			// 条件 SQL 语句
};

// 数据源
struct DataSource
{
public:
	DataSource(): isValid(false)
	{}
	DataSource(const DataSource& ds)
	:isValid(ds.isValid)
	,srcTabName(ds.srcTabName)
	{}

	const DataSource& operator = (const DataSource& ds)
	{
		if ( this != &ds )
		{
			this->isValid    = ds.isValid;
			this->srcTabName = ds.srcTabName;
		}

		return *this;
	}

public:
	bool		isValid;			// 是否有效
	std::string	srcTabName;			// 数据源表名
};

// 采集规则任务信息
struct AcqTaskInfo
{
public:
	// 采集条件类型
	enum EtlConditionType
	{
		ETLCTYPE_UNKNOWN				= 0,		// 未知类型
		ETLCTYPE_NONE					= 1,		// 不带条件
		ETLCTYPE_STRAIGHT				= 2,		// 直接条件
		ETLCTYPE_OUTER_JOIN				= 3,		// 外连条件
		ETLCTYPE_OUTER_JOIN_WITH_COND	= 4,		// 外连加条件
	};

	// 数据源类型
	enum DataSourceType
	{
		DSTYPE_UNKNOWN	= 0,			// 未知类型
		DSTYPE_HIVE		= 1,			// HIVE类型
		DSTYPE_DB2		= 2,			// DB2类型
	};

public:
	AcqTaskInfo(): EtlCondType(ETLCTYPE_UNKNOWN), DataSrcType(DSTYPE_UNKNOWN)
	{}

	AcqTaskInfo(const AcqTaskInfo& info)
	:EtlRuleID(info.EtlRuleID)
	,KpiID(info.KpiID)
	,EtlRuleTime(info.EtlRuleTime)
	,EtlRuleType(info.EtlRuleType)
	,EtlRuleTarget(info.EtlRuleTarget)
	,EtlCondType(info.EtlCondType)
	,EtlCondition(info.EtlCondition)
	,DataSrcType(info.DataSrcType)
	,vecEtlRuleDataSrc(info.vecEtlRuleDataSrc)
	,vecEtlRuleDim(info.vecEtlRuleDim)
	,vecEtlRuleVal(info.vecEtlRuleVal)
	{
		//this->vecEtlRuleDataSrc.insert(this->vecEtlRuleDataSrc.begin(), info.vecEtlRuleDataSrc.begin(), info.vecEtlRuleDataSrc.end());
		//this->vecEtlRuleDim.insert(this->vecEtlRuleDim.begin(), info.vecEtlRuleDim.begin(), info.vecEtlRuleDim.end());
		//this->vecEtlRuleVal.insert(this->vecEtlRuleVal.begin(), info.vecEtlRuleVal.begin(), info.vecEtlRuleVal.end());
	}

public:
	const AcqTaskInfo& operator = (const AcqTaskInfo& info)
	{
		if ( this != &info )
		{
			this->EtlRuleID     = info.EtlRuleID    ;
			this->KpiID         = info.KpiID        ;
			this->EtlRuleTime   = info.EtlRuleTime  ;
			this->EtlRuleType   = info.EtlRuleType  ;
			this->EtlRuleTarget = info.EtlRuleTarget;
			this->EtlCondType	= info.EtlCondType  ;
			this->EtlCondition	= info.EtlCondition ;
			this->DataSrcType	= info.DataSrcType ;

			//this->vecEtlRuleDataSrc.insert(this->vecEtlRuleDataSrc.begin(), info.vecEtlRuleDataSrc.begin(), info.vecEtlRuleDataSrc.end());
			//this->vecEtlRuleDim.insert(this->vecEtlRuleDim.begin(), info.vecEtlRuleDim.begin(), info.vecEtlRuleDim.end());
			//this->vecEtlRuleVal.insert(this->vecEtlRuleVal.begin(), info.vecEtlRuleVal.begin(), info.vecEtlRuleVal.end());
			this->vecEtlRuleDataSrc = info.vecEtlRuleDataSrc;
			this->vecEtlRuleDim     = info.vecEtlRuleDim;
			this->vecEtlRuleVal     = info.vecEtlRuleVal;
		}

		return *this;
	}

	void Clear()
	{
		EtlRuleID.clear();
		KpiID.clear();
		EtlRuleTime.clear();
		EtlRuleType.clear();
		EtlRuleTarget.clear();
		EtlCondType = ETLCTYPE_UNKNOWN;
		EtlCondition.clear();
		DataSrcType = DSTYPE_UNKNOWN;

		std::vector<DataSource>().swap(vecEtlRuleDataSrc);
		std::vector<AcqEtlDim>().swap(vecEtlRuleDim);
		std::vector<AcqEtlVal>().swap(vecEtlRuleVal);
	}

	// 设置采集条件类型
	bool SetConditionType(const std::string& type)
	{
		std::string c_type = base::PubStr::TrimUpperB(type);

		if ( "NONE" == c_type )		// 不带条件
		{
			EtlCondType = ETLCTYPE_NONE;
		}
		else if ( "STRAIGHT" == c_type )	// 直接条件
		{
			EtlCondType = ETLCTYPE_STRAIGHT;
		}
		else if ( "OUTER_JOIN" == c_type )	// 外连条件
		{
			EtlCondType = ETLCTYPE_OUTER_JOIN;
		}
		else if ( "OUTER_JOIN_WITH_COND" == c_type )
		{
			EtlCondType = ETLCTYPE_OUTER_JOIN_WITH_COND;
		}
		else	// ERROR: 未知条件
		{
			EtlCondType = ETLCTYPE_UNKNOWN;
			return false;
		}

		return true;
	}

	// 设置数据源类型
	bool SetDataSourceType(const std::string& type)
	{
		std::string ds_type = base::PubStr::TrimUpperB(type);

		if ( "HIVE" == ds_type )		// HIVE类型
		{
			DataSrcType = DSTYPE_HIVE;
		}
		else if ( "DB2" == ds_type )		// DB2类型
		{
			DataSrcType = DSTYPE_DB2;
		}
		else	// ERROR: 未知类型
		{
			DataSrcType = DSTYPE_UNKNOWN;
			return false;
		}

		return true;
	}

public:
	std::string			EtlRuleID;					// 采集规则ID
	std::string			KpiID;						// 指标ID
	std::string			EtlRuleTime;				// 采集时间(周期)
	std::string			EtlRuleType;				// 采集处理方式
	std::string			EtlRuleTarget;				// 采集目标数据存放
	EtlConditionType	EtlCondType;				// 采集条件类型
	std::string			EtlCondition;				// 采集条件
	DataSourceType		DataSrcType;				// 数据源类型

	std::vector<DataSource>		vecEtlRuleDataSrc;			// 采集数据源
	std::vector<AcqEtlDim>		vecEtlRuleDim;				// 采集维度信息
	std::vector<AcqEtlVal>		vecEtlRuleVal;				// 采集值信息

	std::map<int, EtlSrcInfo>	mapEtlSrc;			// 采集数据源信息
};

// 业财稽核因子规则信息
struct YCInfo
{
public:
	std::string	stat_id;
	std::string stat_sql;
};

