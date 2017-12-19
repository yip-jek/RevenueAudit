#include "ycstatarithmet.h"
#include "pubstr.h"

YCStatArithmet::YCStatArithmet()
{
}

YCStatArithmet::~YCStatArithmet()
{
}

void Arithmetic::Calc(const std::string& expr)
{
	Convert(expr);
	DoCalc();
}

void Arithmetic::Convert(const std::string& expr)
{
	Clear();

	const std::string EXP = NoSpaces(expr);

	int         index = 0;
	std::string one;

	while ( (index = GetOne(EXP, index, one)) >= 0 )
	{
		m_vecOut.push_back(one);
	}

	if ( RPN() )
	{
		std::cout << "Convert to: " << ListOut() << std::endl;
	}
}

void Arithmetic::DoCalc()
{
	if ( m_vecOut.empty() )
	{
		std::cerr << "[CALC] m_vecOut is empty !!!" << std::endl;
	}
	else
	{
		while ( m_vecOut.size() > 1 )
		{
			if ( !CalcOnce() )
			{
				std::cerr << "[CALC] CalcOnce failed !!!" << std::endl;
				return;
			}
		}

		std::cout << "Calc result: " << base::PubStr::StringDoubleFormat(m_vecOut[0]) << std::endl;
	}
}

void Arithmetic::Clear()
{
	// Clear stack
	while ( !m_stackOper.empty() )
	{
		m_stackOper.pop();
	}

	// Clear vector
	std::vector<std::string>().swap(m_vecOut);
}

std::string Arithmetic::NoSpaces(const std::string& expr)
{
	std::string n_exp;

	const int EXP_SIZE = expr.size();
	for ( int i = 0; i < EXP_SIZE; ++i )
	{
		if ( expr[i] != '\x20' )
		{
			n_exp += expr[i];
		}
	}

	return n_exp;
}

bool Arithmetic::IsOper(const std::string& str)
{
	return ( "+" == str 
			|| "-" == str 
			|| "*" == str 
			|| "/" == str 
			|| "(" == str 
			|| ")" == str);
}

int Arithmetic::GetOne(const std::string& expr, int index, std::string& one)
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

bool Arithmetic::RPN()
{
	std::vector<std::string> vec_out;
	vec_out.swap(m_vecOut);

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
			m_vecOut.push_back(ref_str);
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

		m_vecOut.push_back(str);
	}

	return true;
}

void Arithmetic::DealWithOper(const std::string& oper)
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
		//		m_vecOut.push_back(str);
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
			//		m_vecOut.push_back(str);
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
			//		m_vecOut.push_back(str);
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

void Arithmetic::PopStack(pFunIsCondition fun_cond)
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
			m_vecOut.push_back(str);
		}
	}
}

bool Arithmetic::IsLeftParenthesis(const std::string& str)
{
	return ("(" == str);
}

bool Arithmetic::IsLowLevelOper(const std::string& str)
{
	return ("(" == str || "+" == str || "-" == str);
}

std::string Arithmetic::OperatePlus(const std::string& left, const std::string& right)
{
	double d_left  = 0.0;
	double d_right = 0.0;

	// Error !
	if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	{
		//std::cout << "OperatePlus failed !!!" << std::endl;
		return std::string();
	}

	//std::cout << "OperatePlus: double_left=" << d_left << ", double_right=" << d_right << std::endl;
	d_left += d_right;
	//std::cout << "OperatePlus: left=" << left << ", right=" << right << ", result=" << d_left << std::endl;
	return base::PubStr::Double2Str(d_left);
}

std::string Arithmetic::OperateMinus(const std::string& left, const std::string& right)
{
	double d_left  = 0.0;
	double d_right = 0.0;

	// Error !
	if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	{
		//std::cout << "OperateMinus failed !!!" << std::endl;
		return std::string();
	}

	//std::cout << "OperateMinus: double_left=" << d_left << ", double_right=" << d_right << std::endl;
	d_left -= d_right;
	//std::cout << "OperateMinus: left=" << left << ", right=" << right << ", result=" << d_left << std::endl;
	return base::PubStr::Double2Str(d_left);
}

std::string Arithmetic::OperateMultiply(const std::string& left, const std::string& right)
{
	double d_left  = 0.0;
	double d_right = 0.0;

	// Error !
	if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	{
		//std::cout << "OperateMultiply failed !!!" << std::endl;
		return std::string();
	}

	//std::cout << "OperateMultiply: double_left=" << d_left << ", double_right=" << d_right << std::endl;
	d_left *= d_right;
	//std::cout << "OperateMultiply: left=" << left << ", right=" << right << ", result=" << d_left << std::endl;
	return base::PubStr::Double2Str(d_left);
}

std::string Arithmetic::OperateDivide(const std::string& left, const std::string& right)
{
	double d_left  = 0.0;
	double d_right = 0.0;

	// Error !
	if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	{
		//std::cout << "OperateDivide failed !!!" << std::endl;
		return std::string();
	}

	//std::cout << "OperateDivide: double_left=" << d_left << ", double_right=" << d_right << std::endl;
	d_left /= d_right;
	//std::cout << "OperateDivide: left=" << left << ", right=" << right << ", result=" << d_left << std::endl;
	return base::PubStr::Double2Str(d_left);
}

std::string Arithmetic::ListOut()
{
	std::string out;

	const int VEC_SIZE = m_vecOut.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		//std::cout << "_vec[" << (i+1) << "]=" << m_vecOut[i] << std::endl;
		out += m_vecOut[i] + " ";
	}

	//std::cout << std::endl;
	return out;
}

bool Arithmetic::CalcOnce()
{
	const int VEC_SIZE = m_vecOut.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		std::string& ref_out = m_vecOut[i];

		pFunOperate pfun_op = GetOperator(ref_out);
		if ( pfun_op != NULL )
		{
			if ( i < 2 )	// 少于2个操作数
			{
				return false;
			}

			std::string result = (*pfun_op)(m_vecOut[i-2], m_vecOut[i-1]);

			m_vecOut.erase(m_vecOut.begin()+i-2, m_vecOut.begin()+i+1);
			m_vecOut.insert(m_vecOut.begin()+i-2, result);
			return true;
		}
	}

	return false;
}

Arithmetic::pFunOperate Arithmetic::GetOperator(const std::string& oper)
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

