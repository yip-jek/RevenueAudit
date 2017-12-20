#include "ycstatarithmet.h"
#include "pubstr.h"
#include "anaerror.h"


YCOutputItem::YCOutputItem(OutputItemType type /*= OIT_Unknown*/)
:m_type(type)
{
}

YCOutputItem::~YCOutputItem()
{
}

void YCOutputItem::SetType(OutputItemType type)
{
	m_type = type;
}

OutputItemType YCOutputItem::GetType() const
{
	return m_type;
}

std::string YCOutputItem::GetOutputItem(size_t index) const
{
	if ( index < m_vecOutputItem.size() )
	{
		return m_vecOutputItem[index];
	}

	return std::string();
}

size_t YCOutputItem::GetItemSize() const
{
	return m_vecOutputItem.size();
}

////////////////////////////////////////////////////////////////////////////////
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
}

void YCStatArithmet::Calculate(std::vector<std::string>& vec_val) throw(base::Exception)
{
	Convert(expr);
	DoCalc();
}

void YCStatArithmet::Convert(const std::string& expr)
{
	Clear();

	const std::string EXP = NoSpaces(expr);

	int         index = 0;
	std::string one;

	while ( (index = GetOne(EXP, index, one)) >= 0 )
	{
		m_vecOutputItem.push_back(one);
	}

	if ( RPN() )
	{
		std::cout << "Convert to: " << ListOut() << std::endl;
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

void YCStatArithmet::Clear()
{
	// Clear stack
	while ( !m_stackOper.empty() )
	{
		m_stackOper.pop();
	}

	// Clear vector
	std::vector<std::string>().swap(m_vecOutputItem);
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

int YCStatArithmet::GetOne(const std::string& expr, int index, std::string& one)
{
	const int EXP_SIZE = expr.size();
	if ( index >= EXP_SIZE )
	{
		return -1;
	}

	int n_idx = index;
	while ( n_idx < EXP_SIZE )
	{
		const char CH = expr[n_idx];
		if ( IsOper(std::string(1, CH)) )
		{
			if ( index == n_idx )
			{
				one = CH;

				return (++n_idx);
			}
			else
			{
				break;
			}
		}

		++n_idx;
	}

	one = expr.substr(index, n_idx-index);
	return n_idx;
}

bool YCStatArithmet::RPN()
{
	std::vector<std::string> vec_out;
	vec_out.swap(m_vecOutputItem);

	const int VEC_SIZE = vec_out.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		const std::string& ref_str = vec_out[i];

		if ( IsOper(ref_str) )
		{
			if ( m_stackOper.empty() )
			{
				m_stackOper.push(ref_str);
			}
			else
			{
				DealWithOper(ref_str);
			}
		}
		else
		{
			m_vecOutputItem.push_back(ref_str);
		}
	}

	std::string str;
	while ( !m_stackOper.empty() )
	{
		str = m_stackOper.top();
		m_stackOper.pop();

		// syntax error
		// 注意：最后全部pop出时，还存在左括号，则表明表达式括号不匹配，错误 !!!
		if ( "(" == str )
		{
			std::cerr << "SYNTAX ERROR: Parentheses do not match !!!" << std::endl;
			return false;
		}

		m_vecOutputItem.push_back(str);
	}

	return true;
}

void YCStatArithmet::DealWithOper(const std::string& oper)
{
	if ( "(" == oper )
	{
		m_stackOper.push(oper);
	}
	else if ( ")" == oper )
	{
		//while ( !m_stackOper.empty() )
		//{
		//	str = m_stackOper.top();
		//	m_stackOper.pop();

		//	if ( "(" == str )
		//	{
		//		break;
		//	}
		//	else
		//	{
		//		m_vecOutputItem.push_back(str);
		//	}
		//}
		PopStack(&IsLeftParenthesis);

		// POP 出左括号
		if ( !m_stackOper.empty() )
		{
			m_stackOper.pop();
		}
	}
	else
	{
		if ( "+" == oper || "-" == oper )	// 低级运算符："+"和"-"
		{
			//while ( !m_stackOper.empty() )
			//{
			//	str = m_stackOper.top();

			//	if ( "(" == str )
			//	{
			//		break;
			//	}
			//	else
			//	{
			//		m_stackOper.pop();
			//		m_vecOutputItem.push_back(str);
			//	}
			//}
			PopStack(&IsLeftParenthesis);
		}
		else	// 高级运算符："*"和"/"
		{
			//while ( !m_stackOper.empty() )
			//{
			//	str = m_stackOper.top();

			//	if ( "*" == str || "/" == str )
			//	{
			//		m_stackOper.pop();
			//		m_vecOutputItem.push_back(str);
			//	}
			//	else
			//	{
			//		break;
			//	}
			//}
			PopStack(&IsLowLevelOper);
		}

		m_stackOper.push(oper);
	}
}

void YCStatArithmet::PopStack(pFunIsCondition fun_cond)
{
	std::string str;

	while ( !m_stackOper.empty() )
	{
		str = m_stackOper.top();

		if ( (*fun_cond)(str) )
		{
			break;
		}
		else
		{
			m_stackOper.pop();
			m_vecOutputItem.push_back(str);
		}
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

