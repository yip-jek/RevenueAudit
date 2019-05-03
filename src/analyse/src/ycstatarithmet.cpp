#include "ycstatarithmet.h"
#include "pubstr.h"
#include "anaerror.h"
#include "ycstatfactor.h"


bool YCStatArithmet::IsLeftParenthesis(const std::string& str)
{
	return ("(" == str);
}

bool YCStatArithmet::IsLowLevelOper(const std::string& str)
{
	return ("(" == str || "+" == str || "-" == str);
}

std::string YCStatArithmet::Operate(const std::string& left, const std::string& right, const char op)
{
	double d_left  = 0.0;
	double d_right = 0.0;

	if ( left.empty() && right.empty() )
	{
		return "";
	}

	// Left: 转换失败，默认取 0
	if ( !left.empty() && !base::PubStr::Str2Double(left, d_left) )
	{
		d_left = 0.0;
	}

	// Right: 转换失败，默认取 0
	if ( !right.empty() && !base::PubStr::Str2Double(right, d_right) )
	{
		d_right = 0.0;
	}

	switch ( op )
	{
	case '+': d_left += d_right; break;
	case '-': d_left -= d_right; break;
	case '*': d_left *= d_right; break;
	case '/': d_left /= d_right; break;
	default:
		throw base::Exception(ANAERR_OPERATE_FAILED, "Operate failed! Unknown operator type: [%c] [FILE:%s, LINE:%d]", op, __FILE__, __LINE__);
	}

	return base::PubStr::Double2Str(d_left);
}

std::string YCStatArithmet::OperatePlus(const std::string& left, const std::string& right)
{
	return Operate(left, right, '+');

	//double d_left  = 0.0;
	//double d_right = 0.0;

	//// Error !
	//if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	//{
	//	throw base::Exception(ANAERR_OPERATE_FAILED, "Operate plus failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	//}

	//d_left += d_right;
	//return base::PubStr::Double2Str(d_left);
}

std::string YCStatArithmet::OperateMinus(const std::string& left, const std::string& right)
{
	return Operate(left, right, '-');

	//double d_left  = 0.0;
	//double d_right = 0.0;

	//// Error !
	//if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	//{
	//	throw base::Exception(ANAERR_OPERATE_FAILED, "Operate minus failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	//}

	//d_left -= d_right;
	//return base::PubStr::Double2Str(d_left);
}

std::string YCStatArithmet::OperateMultiply(const std::string& left, const std::string& right)
{
	return Operate(left, right, '*');

	//double d_left  = 0.0;
	//double d_right = 0.0;

	//// Error !
	//if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	//{
	//	throw base::Exception(ANAERR_OPERATE_FAILED, "Operate multiply failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	//}

	//d_left *= d_right;
	//return base::PubStr::Double2Str(d_left);
}

std::string YCStatArithmet::OperateDivide(const std::string& left, const std::string& right)
{
	return Operate(left, right, '/');

	//double d_left  = 0.0;
	//double d_right = 0.0;

	//// Error !
	//if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	//{
	//	throw base::Exception(ANAERR_OPERATE_FAILED, "Operate divide failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	//}

	//d_left /= d_right;
	//return base::PubStr::Double2Str(d_left);
}

YCStatArithmet::YCStatArithmet(YCStatFactor* p_stat_factor)
:m_pStatFactor(p_stat_factor)
{
}

YCStatArithmet::~YCStatArithmet()
{
}

void YCStatArithmet::Load(const std::string& expression)
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

void YCStatArithmet::Calculate(std::vector<double>& vec_val)
{
	if ( m_vecOutputItem.empty() )
	{
		throw base::Exception(ANAERR_ARITHMET_CALCULATE, "The output item vector is empty! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( NULL == m_pStatFactor )
	{
		throw base::Exception(ANAERR_ARITHMET_CALCULATE, "The pointer of YCStatFactor is NULL! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 从输出项队列中，取出数据项与运算符进行四则运算
	// 直至得出最终的结果
	do
	{
		CalcOnce();
	}
	while ( m_vecOutputItem.size() > 1 );

	// 计算得出的最终结果
	OutputResult(vec_val);
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

void YCStatArithmet::Convert2Postorder(const std::string& expression)
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

void YCStatArithmet::Postorder(const std::vector<std::string>& vec_element)
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

void YCStatArithmet::DealWithOper(const std::string& oper)
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

void YCStatArithmet::CalcOnce()
{
	const int VEC_SIZE = m_vecOutputItem.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YCOutputItem& ref_item = m_vecOutputItem[i];

		// 未知类型
		if ( YCOutputItem::OIT_UNKNOWN == ref_item.item_type )
		{
			throw base::Exception(ANAERR_ARITHMET_CALCULATE, "Unknown output item type! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		}

		// 是否为运算符？
		if ( ref_item.item_type != YCOutputItem::OIT_OPERATOR )
		{
			continue;
		}

		// 直到取到第一个运算符，才进行计算
		pFunOperate pfun_oper = GetOperate(ref_item.vec_out[0]);
		if ( NULL == pfun_oper )
		{
			throw base::Exception(ANAERR_ARITHMET_CALCULATE, "NO operate for operator: [%s] [FILE:%s, LINE:%d]", ref_item.vec_out[0].c_str(), __FILE__, __LINE__);
		}

		// 是否少于 2 个操作数？
		if ( i < 2 )
		{
			throw base::Exception(ANAERR_ARITHMET_CALCULATE, "Less than 2 operands: [%d] [FILE:%s, LINE:%d]", i, __FILE__, __LINE__);
		}

		// 结果计算
		YCOutputItem item_result;
		OperateResult(m_vecOutputItem[i-2], m_vecOutputItem[i-1], pfun_oper, item_result);

		// 参与运算的元素全部删除，并将结果插入到删除元素的起始位置
		m_vecOutputItem.erase(m_vecOutputItem.begin()+i-2, m_vecOutputItem.begin()+i+1);
		m_vecOutputItem.insert(m_vecOutputItem.begin()+i-2, item_result);
		return;
	}

	// 运算符不存在，无法进行计算
	throw base::Exception(ANAERR_ARITHMET_CALCULATE, "The operator: NOT FOUND! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
}

YCStatArithmet::pFunOperate YCStatArithmet::GetOperate(const std::string& oper)
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

void YCStatArithmet::OperateResult(YCOutputItem& item_left, YCOutputItem& item_right, pFunOperate pfun_oper, YCOutputItem& item_result)
{
	if ( YCOutputItem::OIT_CONSTANT == item_left.item_type 
		&& YCOutputItem::OIT_CONSTANT == item_right.item_type )	// 左右操作数同为常量
	{
		std::string data = (*pfun_oper)(item_left.vec_out[0], item_right.vec_out[0]);

		item_result.item_type = YCOutputItem::OIT_CONSTANT;
		item_result.vec_out.assign(1, data);
	}
	else
	{
		// 通过维度找到对应的数值
		TryGetDimDataValue(item_left);
		TryGetDimDataValue(item_right);

		// 此时，输出项类型只存在2种情况："常量类型"或者"数值类型"
		// 且至少有一个输出项类型为"数值类型"
		TryMatchOutputItem(item_right, item_left);
		TryMatchOutputItem(item_left, item_right);

		OperateValue(item_left, item_right, pfun_oper, item_result);
	}
}

void YCStatArithmet::TryGetDimDataValue(YCOutputItem& item)
{
	if ( YCOutputItem::OIT_DIM == item.item_type )
	{
		m_pStatFactor->GetDimFactorValue(item.vec_out[0], item.vec_out);
		item.item_type = YCOutputItem::OIT_VALUE;
	}
}

void YCStatArithmet::TryMatchOutputItem(const YCOutputItem& item_val, YCOutputItem& item_const)
{
	// 是否为常量类型？
	if ( YCOutputItem::OIT_CONSTANT == item_const.item_type )
	{
		// 匹配
		size_t off_size = item_val.vec_out.size() - item_const.vec_out.size();
		if ( off_size > 0 )
		{
			item_const.vec_out.insert(item_const.vec_out.end(), off_size, item_const.vec_out[0]);
		}
	}
}

void YCStatArithmet::OperateValue(YCOutputItem& item_left, YCOutputItem& item_right, pFunOperate pfun_oper, YCOutputItem& item_result)
{
	const int VEC_SIZE = item_left.vec_out.size();
	if ( (size_t)VEC_SIZE != item_right.vec_out.size() )
	{
		throw base::Exception(ANAERR_ARITHMET_CALCULATE, "The size of output items do not match: Left output item size [%lu], right output item size [%lu] [FILE:%s, LINE:%d]", VEC_SIZE, item_right.vec_out.size(), __FILE__, __LINE__);
	}

	std::vector<std::string> vec_val;
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		vec_val.push_back((*pfun_oper)(item_left.vec_out[i], item_right.vec_out[i]));
	}

	item_result.item_type = YCOutputItem::OIT_VALUE;
	item_result.vec_out.swap(vec_val);
}

void YCStatArithmet::OutputResult(std::vector<double>& vec_result)
{
	double d_val = 0.0;
	std::vector<double> vec_val;

	std::vector<std::string>& ref_vec_out = m_vecOutputItem[0].vec_out;
	const int VEC_SIZE = ref_vec_out.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		if ( !base::PubStr::Str2Double(ref_vec_out[i], d_val) )
		{
			//throw base::Exception(ANAERR_ARITHMET_CALCULATE, "无法转换为精度类型的结果值：[%s] [FILE:%s, LINE:%d]", ref_vec_out[i].c_str(), __FILE__, __LINE__);

			// 无法转换为精度类型，默认取 0
			d_val = 0.0;
		}

		vec_val.push_back(d_val);
	}

	vec_val.swap(vec_result);
}

