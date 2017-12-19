#pragma once

#include <stack>
#include <vector>
#include "exception.h"

class YCStatFactor;

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
	static std::string OperatePlus(const std::string& left, const std::string& right);
	// 减运算
	static std::string OperateMinus(const std::string& left, const std::string& right);
	// 乘运算
	static std::string OperateMultiply(const std::string& left, const std::string& right);
	// 除运算
	static std::string OperateDivide(const std::string& left, const std::string& right);

public:
	YCStatArithmet(const YCStatFactor& stat_factor);
	~YCStatArithmet();

	// 载入算术表达式
	void Load(const std::string& expression) throw(base::Exception);

	void Calc(const std::string& expr);

private:
	// 算术表达式后序转换
	void Convert(const std::string& expr);

	void DoCalc();

	void Clear();

	std::string NoSpaces(const std::string& expr);

	bool IsOper(const std::string& str);

	int GetOne(const std::string& expr, int index, std::string& one);

	bool RPN();

	void DealWithOper(const std::string& oper);

	void PopStack(pFunIsCondition fun_cond);

	std::string ListOut();

	bool CalcOnce();

	pFunOperate GetOperator(const std::string& oper);

private:
	const YCStatFactor*      m_pStatFactor;
	std::stack<std::string>  m_stackOper;
	std::vector<std::string> m_vecOut;
};

