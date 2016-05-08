#pragma once

#include <string>
#include <vector>
#include <boost/algorithm/string.hpp>

// 单个维度信息
struct OneEtlDim
{
public:
	OneEtlDim(): EtlDimSeq(0)
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
		EtlDimMemo.clear();
	}

public:
	std::string	EtlDimID;					// 采集维度ID
	int			EtlDimSeq;					// 采集维度序号
	std::string	EtlDimName;					// 采集维度名称
	std::string	EtlDimDesc;					// 采集维度描述
	std::string	EtlDimSrcName;				// 维度对应源字段名称
	std::string EtlDimMemo;					// 备注
};

// 采集维度信息
struct AcqEtlDim
{
public:
	AcqEtlDim()
	{}

	AcqEtlDim(const AcqEtlDim& dim)
	:acqEtlDimID(dim.acqEtlDimID)
	,vecEtlDim(dim.vecEtlDim)
	{
		//this->vecEtlDim.insert(this->vecEtlDim.begin(), dim.vecEtlDim.begin(), dim.vecEtlDim.end());
	}

public:
	const AcqEtlDim& operator = (const AcqEtlDim& dim)
	{
		if ( this != &dim )
		{
			this->acqEtlDimID = dim.acqEtlDimID;

			//this->vecEtlDim.insert(this->vecEtlDim.begin(), dim.vecEtlDim.begin(), dim.vecEtlDim.end());
			this->vecEtlDim   = dim.vecEtlDim;
		}

		return *this;
	}

	void Clear()
	{
		acqEtlDimID.clear();

		std::vector<OneEtlDim>().swap(vecEtlDim);
	}

public:
	std::string				acqEtlDimID;		// 采集维度ID
	std::vector<OneEtlDim>	vecEtlDim;			// 维度信息集
};

// 单个采集值信息
struct OneEtlVal
{
public:
	OneEtlVal(): EtlValSeq(0)
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
		EtlValMemo.clear();
	}

public:
	std::string	EtlValID;					// 采集值ID
	int			EtlValSeq;					// 采集值序号
	std::string	EtlValName;					// 采集值名称
	std::string	EtlValDesc;					// 采集值描述
	std::string	EtlValSrcName;				// 值对应源字段名称
	std::string	EtlValMemo;					// 备注
};

// 采集值信息
struct AcqEtlVal
{
public:
	AcqEtlVal()
	{}

	AcqEtlVal(const AcqEtlVal& val)
	:acqEtlValID(val.acqEtlValID)
	,vecEtlVal(val.vecEtlVal)
	{
		//this->vecEtlVal.insert(this->vecEtlVal.begin(), val.vecEtlVal.begin(), val.vecEtlVal.end());
	}

public:
	const AcqEtlVal& operator = (const AcqEtlVal& val)
	{
		if ( this != &val )
		{
			this->acqEtlValID = val.acqEtlValID;

			//this->vecEtlVal.insert(this->vecEtlVal.begin(), val.vecEtlVal.begin(), val.vecEtlVal.end());
			this->vecEtlVal   = val.vecEtlVal;
		}

		return *this;
	}

	void Clear()
	{
		acqEtlValID.clear();

		std::vector<OneEtlVal>().swap(vecEtlVal);
	}

public:
	std::string				acqEtlValID;		// 采集值ID
	std::vector<OneEtlVal>	vecEtlVal;			// 值信息集
};

// 采集规则任务信息
struct AcqTaskInfo
{
public:
	// 采集条件类型
	enum EtlConditionType
	{
		ETLCTYPE_UNKNOWN	= 0,		// 未知类型
		ETLCTYPE_NONE		= 1,		// 不带条件
		ETLCTYPE_STRAIGHT	= 2,		// 直接条件
		ETLCTYPE_OUTER_JOIN	= 3,		// 外连条件
	};

public:
	AcqTaskInfo(): EtlCondType(ETLCTYPE_UNKNOWN)
	{}

	AcqTaskInfo(const AcqTaskInfo& info)
	:EtlRuleID(info.EtlRuleID)
	,KpiID(info.KpiID)
	,EtlRuleTime(info.EtlRuleTime)
	,EtlRuleType(info.EtlRuleType)
	,EtlRuleTarget(info.EtlRuleTarget)
	,EtlCondType(info.EtlCondType)
	,EtlCondition(info.EtlCondition)
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

		std::vector<std::string>().swap(vecEtlRuleDataSrc);
		std::vector<AcqEtlDim>().swap(vecEtlRuleDim);
		std::vector<AcqEtlVal>().swap(vecEtlRuleVal);
	}

	bool SetConditionType(std::string c_type)
	{
		boost::trim(c_type);
		boost::to_upper(c_type);

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
		else	// ERROR: 未知条件
		{
			EtlCondType = ETLCTYPE_UNKNOWN;
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

	std::vector<std::string>	vecEtlRuleDataSrc;			// 采集数据源
	std::vector<AcqEtlDim>		vecEtlRuleDim;				// 采集维度信息
	std::vector<AcqEtlVal>		vecEtlRuleVal;				// 采集值信息
};

