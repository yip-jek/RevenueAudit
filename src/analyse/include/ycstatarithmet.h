#pragma once

#include <stack>
#include <vector>
#include "exception.h"

class YCStatFactor;

// 输出项类
class YCOutputItem
{
public:
	// 输出项类型
	enum OutputItemType
	{
		OIT_Unknown    = 0,				// 未知类型（初始值）
		OIT_DIM        = 1,				// 维度ID
		OIT_OPERATOR   = 2,				// 运算符
		OIT_DATE_VALUE = 3,				// 数值（可参与运算）
	};

	YCOutputItem(OutputItemType type = OIT_Unknown);
	~YCOutputItem();

public:
	// 设置类型
	void SetType(OutputItemType type);

	// 获取类型
	OutputItemType GetType() const;

	// 获取输出项
	std::string GetOutputItem(size_t index) const;

	// 获取输出项个数
	size_t GetItemSize() const;

private:
	OutputItemType           m_type;
	std::vector<std::string> m_vecOut;
};


////////////////////////////////////////////////////////////////////////////////
// （业财）统计（四则）运算类
class YCStatArithmet
{
public:
	typedef bool (*pFunIsCondition)(const std::string&);
	typedef std::string (*pFunOperate)(const std::string&, const std::string&);

	// 是否为左括号？
	static bool IsLeftParenthesis(const std::string& str);

	// 是否为低级运算符？
	// "+"和"-"为低级运算符
	// "("为特殊运算符，也算作低级运算符
	static bool IsLowLevelOper(const std::string& str);

	// 加运算
	static std::string OperatePlus(const std::string& left, const std::string& right) throw(base::Exception);
	// 减运算
	static std::string OperateMinus(const std::string& left, const std::string& right) throw(base::Exception);
	// 乘运算
	static std::string OperateMultiply(const std::string& left, const std::string& right) throw(base::Exception);
	// 除运算
	static std::string OperateDivide(const std::string& left, const std::string& right) throw(base::Exception);

public:
	YCStatArithmet(YCStatFactor* p_stat_factor);
	~YCStatArithmet();

	// 载入四则运算表达式
	void Load(const std::string& expression) throw(base::Exception);

	// 生成计算结果
	void Calculate(std::vector<std::string>& vec_val) throw(base::Exception);

private:
	// 算术表达式后序转换
	void Convert(const std::string& expr);

	void DoCalc();

	void Clear();

	bool IsOper(const std::string& str);

	int GetOne(const std::string& expr, int index, std::string& one);

	bool RPN();

	void DealWithOper(const std::string& oper);

	void PopStack(pFunIsCondition fun_cond);

	std::string ListOut();

	bool CalcOnce();

	pFunOperate GetOperator(const std::string& oper);

private:
	YCStatFactor*             m_pStatFactor;
	std::stack<std::string>   m_stackOper;
	std::vector<YCOutputItem> m_vecOutputItem;
};

