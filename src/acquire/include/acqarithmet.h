#pragma once

#include <map>
#include <stack>
#include <vector>
#include "exception.h"

// 表达式信息
struct ExprInfo
{
public:
	ExprInfo(): index(-1) {}

public:
	int         index;				// 所在索引位置
	std::string expr;				// 表达式
};

// 输出项类
class YCOutputItem
{
public:
	// 输出项类型
	enum OutputItemType
	{
		OIT_UNKNOWN  = 0,			// 未知类型（初始值）
		OIT_CONSTANT = 1,			// 常量
		OIT_DIM      = 2,			// 维度ID
		OIT_OPERATOR = 3,			// 运算符
		OIT_VALUE    = 4,			// 数值（可参与运算）
	};

public:
	YCOutputItem(OutputItemType type = OIT_UNKNOWN): item_type(type) {}
	~YCOutputItem() {}

public:
	OutputItemType           item_type;
	std::vector<std::string> vec_out;
};


////////////////////////////////////////////////////////////////////////////////
// （业财）采集（四则）运算类
class AcqArithmet
{
public:
	typedef bool (*pFunIsCondition)(const std::string&);
	typedef std::string (*pFunOperate)(const std::string&, const std::string&);

	static const char* const S_ARITHMETIC_TAG;
	static const char* const S_DATVAL_PREFIX;

	// 是否计算式
	static bool IsArithmetic(const std::string& expr);

	// 是否为左括号？
	static bool IsLeftParenthesis(const std::string& str);

	// 是否为低级运算符？
	// "+"和"-"为低级运算符
	// "("为特殊运算符，也算作低级运算符
	static bool IsLowLevelOper(const std::string& str);

	// 运算
	static std::string Operate(const std::string& left, const std::string& right, const char op) throw(base::Exception);
	// 加运算
	static std::string OperatePlus(const std::string& left, const std::string& right) throw(base::Exception);
	// 减运算
	static std::string OperateMinus(const std::string& left, const std::string& right) throw(base::Exception);
	// 乘运算
	static std::string OperateMultiply(const std::string& left, const std::string& right) throw(base::Exception);
	// 除运算
	static std::string OperateDivide(const std::string& left, const std::string& right) throw(base::Exception);

public:
	AcqArithmet();
	~AcqArithmet();

	// 进行四则运算
	void DoCalculate(std::vector<std::vector<std::string> >& vec2_srcdata) throw(base::Exception);

private:
	// 清空
	void Clear();

	// 载入计算表达式
	void LoadExpression() throw(base::Exception);

	// 进行计算
	void Calculate();

	// 算术表达式后序转换
	void Convert2Postorder(const std::string& expression) throw(base::Exception);

	// 获取单个元素
	int GetElement(const std::string& expression, int index, std::string& element);

	// 是否为运算符
	bool IsOper(const std::string& str);

	// 后序
	void Postorder(const std::vector<std::string>& vec_element) throw(base::Exception);

	// 处理运算符
	void DealWithOper(const std::string& oper) throw(base::Exception);

	// 当达到条件时，pop出堆栈
	void PopStack(pFunIsCondition fun_cond);

	// 计算结果
	std::string CalcResult() throw(base::Exception);

	// 进行一次四则运算
	void CalcOnce() throw(base::Exception);

	// 获取运算符对应的运算操作
	// 不为运算符则返回 NULL
	pFunOperate GetOperate(const std::string& oper);

	// 计算结果
	void OperateResult(YCOutputItem& item_left, YCOutputItem& item_right, pFunOperate pfun_oper, YCOutputItem& item_result) throw(base::Exception);

	// 尝试获取对应的数值
	void TryGetDataValue(YCOutputItem& item) throw(base::Exception);

	// 尝试匹配输出项
	void TryMatchOutputItem(const YCOutputItem& item_val, YCOutputItem& item_const);

	// 数值计算
	void OperateValue(YCOutputItem& item_left, YCOutputItem& item_right, pFunOperate pfun_oper, YCOutputItem& item_result) throw(base::Exception);

private:
	std::vector<std::string>* m_pVecData;					// 源数据
	std::map<int, ExprInfo>   m_mExpression;				// 计算表达式队列
	std::stack<std::string>   m_stackOper;					// 运算符堆栈
	std::vector<YCOutputItem> m_vecOutputItem;				// 输出项队列
};

