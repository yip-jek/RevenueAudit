#include "ycstatarithmet.h"
#include "pubstr.h"
#include "anaerror.h"


bool YCStatArithmet::IsLeftParenthesis(const std::string& str)
{
	return ("(" == str);
}

bool YCStatArithmet::IsLowLevelOper(const std::string& str)
{
	return ("(" == str || "+" == str || "-" == str);
}

std::string YCStatArithmet::OperatePlus(const std::string& left, const std::string& right) throw(base::Exception)
{
	double d_left  = 0.0;
	double d_right = 0.0;

	// Error !
	if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	{
		throw base::Exception(ANAERR_OPERATE_FAILED, "Operate plus failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	}

	d_left += d_right;
	return base::PubStr::Double2Str(d_left);
}

std::string YCStatArithmet::OperateMinus(const std::string& left, const std::string& right) throw(base::Exception)
{
	double d_left  = 0.0;
	double d_right = 0.0;

	// Error !
	if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	{
		throw base::Exception(ANAERR_OPERATE_FAILED, "Operate minus failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	}

	d_left -= d_right;
	return base::PubStr::Double2Str(d_left);
}

std::string YCStatArithmet::OperateMultiply(const std::string& left, const std::string& right) throw(base::Exception)
{
	double d_left  = 0.0;
	double d_right = 0.0;

	// Error !
	if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	{
		throw base::Exception(ANAERR_OPERATE_FAILED, "Operate multiply failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	}

	d_left *= d_right;
	return base::PubStr::Double2Str(d_left);
}

std::string YCStatArithmet::OperateDivide(const std::string& left, const std::string& right) throw(base::Exception)
{
	double d_left  = 0.0;
	double d_right = 0.0;

	// Error !
	if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	{
		throw base::Exception(ANAERR_OPERATE_FAILED, "Operate divide failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	}

	d_left /= d_right;
	return base::PubStr::Double2Str(d_left);
}

YCStatArithmet::YCStatArithmet(YCStatFactor* p_stat_factor)
:m_pStatFactor(p_stat_factor)
{
}

YCStatArithmet::~YCStatArithmet()
{
}

void YCStatArithmet::Load(const std::string& expression) throw(base::Exception)
{
	if ( expression.empty() )
	{
		throw base::Exception(ANAERR_ARITHMET_LOAD, "The arithmetic expression is a blank! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 清理旧数据
	Clear();

	// 转换为后序
	Convert2Postorder(expression);
}

void YCStatArithmet::Calculate(std::vector<std::string>& vec_val) throw(base::Exception)
{
	DoCalc();
}

void YCStatArithmet::Clear()
{
	// Clear stack
	while ( !m_stackOper.empty() )
	{
		m_stackOper.pop();
	}

	// Clear vector
	std::vector<YCOutputItem>().swap(m_vecOutputItem);
}

void YCStatArithmet::Convert2Postorder(const std::string& expression throw(base::Exception))
{
	int         index = 0;
	std::string element;

	// 获取所有元素
	std::vector<std::string> vec_element;
	while ( (index = GetElement(expression, index, element)) >= 0 )
	{
		// 是否为有效元素？
		// 舍弃为空的无效元素
		if ( !element.empty() )
		{
			vec_element.push_back(element);
		}
	}

	// 后序
	Postorder(vec_element);
}

int YCStatArithmet::GetElement(const std::string& expression, int index, std::string& element)
{
	// 索引位置是否合法？
	const int EXP_SIZE = expression.size();
	if ( index >= EXP_SIZE )
	{
		return -1;
	}

	// 查找元素
	// 运算符即是元素，也是分隔
	int n_index = index;
	while ( n_index < EXP_SIZE )
	{
		const char CH = expression[n_index];

		// 是否找到运算符？
		if ( IsOper(std::string(1, CH)) )
		{
			// 是否在第一位？
			if ( index == n_index )
			{
				element = CH;
				return (++n_index);
			}
			else
			{
				break;
			}
		}

		++n_index;
	}

	element = base::PubStr::TrimB(expression.substr(index, n_index-index));
	return n_index;
}

bool YCStatArithmet::IsOper(const std::string& str)
{
	return ( "+" == str 
			|| "-" == str 
			|| "*" == str 
			|| "/" == str 
			|| "(" == str 
			|| ")" == str);
}

void YCStatArithmet::Postorder(const std::vector<std::string>& vec_element) throw(base::Exception)
{
	if ( vec_element.empty() )
	{
		throw base::Exception(ANAERR_ARITHMET_LOAD, "The element vector is empty! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	double       dou_val = 0.0;
	YCOutputItem output_item;

	// 进行后序转换
	const int VEC_SIZE = vec_element.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		const std::string& ref_str = vec_element[i];

		// 是否为运算符？
		if ( IsOper(ref_str) )
		{
			if ( m_stackOper.empty() )		// 直接放入堆栈
			{
				m_stackOper.push(ref_str);
			}
			else
			{
				DealWithOper(ref_str);
			}
		}
		else	// 不是运算符，则直接输出
		{
			// 是否为常量？
			if ( base::PubStr::Str2Double(ref_str, dou_val) )	// 常量
			{
				output_item.item_type = YCOutputItem::OIT_CONSTANT;
			}
			else	// 维度
			{
				output_item.item_type = YCOutputItem::OIT_DIM;
			}

			output_item.vec_out.assign(1, ref_str);
			m_vecOutputItem.push_back(output_item);
		}
	}

	std::string oper;
	while ( !m_stackOper.empty() )
	{
		oper = m_stackOper.top();
		m_stackOper.pop();

		// Syntax error
		// 注意：最后全部pop出时，还存在左括号，则表明表达式括号不匹配，错误 !!!
		if ( "(" == oper )
		{
			throw base::Exception(ANAERR_ARITHMET_LOAD, "SYNTAX ERROR: Parentheses do not match !!! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		}

		output_item.item_type = YCOutputItem::OIT_OPERATOR;
		output_item.vec_out.assign(1, oper);
		m_vecOutputItem.push_back(output_item);
	}
}

void YCStatArithmet::DealWithOper(const std::string& oper) throw(base::Exception)
{
	if ( "(" == oper )
	{
		// 左括号直接压入堆栈
		m_stackOper.push(oper);
	}
	else if ( ")" == oper )
	{
		// pop 出左括号后的所有运算符
		PopStack(&IsLeftParenthesis);

		// 是否有左括号？
		if ( m_stackOper.empty() )
		{
			throw base::Exception(ANAERR_ARITHMET_LOAD, "SYNTAX ERROR: Parentheses do not match !!! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		}
		else
		{
			// POP 出左括号
			m_stackOper.pop();
		}
	}
	else
	{
		if ( "+" == oper || "-" == oper )	// 低级运算符："+"和"-"
		{
			PopStack(&IsLeftParenthesis);
		}
		else	// 高级运算符："*"和"/"
		{
			PopStack(&IsLowLevelOper);
		}

		m_stackOper.push(oper);
	}
}

void YCStatArithmet::PopStack(pFunIsCondition fun_cond)
{
	std::string  oper;
	YCOutputItem output_item(YCOutputItem::OIT_OPERATOR);

	while ( !m_stackOper.empty() )
	{
		oper = m_stackOper.top();

		if ( (*fun_cond)(oper) )
		{
			break;
		}
		else
		{
			m_stackOper.pop();

			output_item.vec_out.assign(1, oper);
			m_vecOutputItem.push_back(output_item);
		}
	}
}








void YCStatArithmet::DoCalc()
{
	if ( m_vecOutputItem.empty() )
	{
		std::cerr << "[CALC] m_vecOutputItem is empty !!!" << std::endl;
	}
	else
	{
		while ( m_vecOutputItem.size() > 1 )
		{
			if ( !CalcOnce() )
			{
				std::cerr << "[CALC] CalcOnce failed !!!" << std::endl;
				return;
			}
		}

		std::cout << "Calc result: " << base::PubStr::StringDoubleFormat(m_vecOutputItem[0]) << std::endl;
	}
}

std::string YCStatArithmet::ListOut()
{
	std::string out;

	const int VEC_SIZE = m_vecOutputItem.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		//std::cout << "_vec[" << (i+1) << "]=" << m_vecOutputItem[i] << std::endl;
		out += m_vecOutputItem[i] + " ";
	}

	//std::cout << std::endl;
	return out;
}

bool YCStatArithmet::CalcOnce()
{
	const int VEC_SIZE = m_vecOutputItem.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		std::string& ref_out = m_vecOutputItem[i];

		pFunOperate pfun_op = GetOperator(ref_out);
		if ( pfun_op != NULL )
		{
			if ( i < 2 )	// 少于2个操作数
			{
				return false;
			}

			std::string result = (*pfun_op)(m_vecOutputItem[i-2], m_vecOutputItem[i-1]);

			m_vecOutputItem.erase(m_vecOutputItem.begin()+i-2, m_vecOutputItem.begin()+i+1);
			m_vecOutputItem.insert(m_vecOutputItem.begin()+i-2, result);
			return true;
		}
	}

	return false;
}

YCStatArithmet::pFunOperate YCStatArithmet::GetOperator(const std::string& oper)
{
	if ( "+" == oper )
	{
		return &OperatePlus;
	}
	else if ( "-" == oper )
	{
		return &OperateMinus;
	}
	else if ( "*" == oper )
	{
		return &OperateMultiply;
	}
	else if ( "/" == oper )
	{
		return &OperateDivide;
	}
	else
	{
		return NULL;
	}
}

