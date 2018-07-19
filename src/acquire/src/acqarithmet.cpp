#include "acqarithmet.h"
#include "pubstr.h"
#include "acqerror.h"


const char* const AcqArithmet::S_ARITHMETIC_TAG = "ACQ_ARITHMET";
const char* const AcqArithmet::S_DATVAL_PREFIX  = "VAL_";

bool AcqArithmet::IsArithmetic(const std::string& expr)
{
	std::string arith_expr = base::PubStr::TrimUpperB(expr);

	const size_t ARITH_TAG_SIZE = std::string(S_ARITHMETIC_TAG).size();
	if ( arith_expr.size() > (ARITH_TAG_SIZE+2) && '[' == arith_expr[0] )
	{
		const size_t POS_END = arith_expr.find(']');
		if ( POS_END != std::string::npos )
		{
			arith_expr = base::PubStr::TrimB(arith_expr.substr(1, POS_END-2));
			return (S_ARITHMETIC_TAG == arith_expr.substr(0, ARITH_TAG_SIZE));
		}
	}

	return false;
}

bool AcqArithmet::IsLeftParenthesis(const std::string& str)
{
	return ("(" == str);
}

bool AcqArithmet::IsLowLevelOper(const std::string& str)
{
	return ("(" == str || "+" == str || "-" == str);
}

std::string AcqArithmet::Operate(const std::string& left, const std::string& right, const char op) throw(base::Exception)
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
		throw base::Exception(ACQERR_OPERATE_FAILED, "Operate failed! Unknown operator type: [%c] [FILE:%s, LINE:%d]", op, __FILE__, __LINE__);
	}

	return base::PubStr::Double2Str(d_left);
}

std::string AcqArithmet::OperatePlus(const std::string& left, const std::string& right) throw(base::Exception)
{
	return Operate(left, right, '+');

	//double d_left  = 0.0;
	//double d_right = 0.0;

	//// Error !
	//if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	//{
	//	throw base::Exception(ACQERR_OPERATE_FAILED, "Operate plus failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	//}

	//d_left += d_right;
	//return base::PubStr::Double2Str(d_left);
}

std::string AcqArithmet::OperateMinus(const std::string& left, const std::string& right) throw(base::Exception)
{
	return Operate(left, right, '-');

	//double d_left  = 0.0;
	//double d_right = 0.0;

	//// Error !
	//if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	//{
	//	throw base::Exception(ACQERR_OPERATE_FAILED, "Operate minus failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	//}

	//d_left -= d_right;
	//return base::PubStr::Double2Str(d_left);
}

std::string AcqArithmet::OperateMultiply(const std::string& left, const std::string& right) throw(base::Exception)
{
	return Operate(left, right, '*');

	//double d_left  = 0.0;
	//double d_right = 0.0;

	//// Error !
	//if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	//{
	//	throw base::Exception(ACQERR_OPERATE_FAILED, "Operate multiply failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	//}

	//d_left *= d_right;
	//return base::PubStr::Double2Str(d_left);
}

std::string AcqArithmet::OperateDivide(const std::string& left, const std::string& right) throw(base::Exception)
{
	return Operate(left, right, '/');

	//double d_left  = 0.0;
	//double d_right = 0.0;

	//// Error !
	//if ( !base::PubStr::Str2Double(left, d_left) || !base::PubStr::Str2Double(right, d_right) )
	//{
	//	throw base::Exception(ACQERR_OPERATE_FAILED, "Operate divide failed! Can not convert to double type: left=[%s], right=[%s] [FILE:%s, LINE:%d]", left.c_str(), right.c_str(), __FILE__, __LINE__);
	//}

	//d_left /= d_right;
	//return base::PubStr::Double2Str(d_left);
}

AcqArithmet::AcqArithmet()
:m_pVecData(NULL)
{
}

AcqArithmet::~AcqArithmet()
{
}

void AcqArithmet::DoCalculate(std::vector<std::vector<std::string> >& vec2_srcdata) throw(base::Exception)
{
	if ( vec2_srcdata.empty() )
	{
		throw base::Exception(ACQERR_ARITHMET_LOAD, "NO source data! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	const int VEC2_SIZE = vec2_srcdata.size();
	for ( int i = 0; i < VEC2_SIZE; ++i )
	{
		// 清理缓存数据
		Clear();

		m_pVecData = &vec2_srcdata[i];

		LoadExpression();
		Calculate();
	}
}

void AcqArithmet::Calculate()
{
	for ( std::map<int, ExprInfo>::iterator it = m_mExpression.begin(); it != m_mExpression.end(); ++it )
	{
		// 转换为后序
		Convert2Postorder(it->second.expr);

		(*m_pVecData)[it->second.index] = CalcResult();
	}
}

void AcqArithmet::Clear()
{
	m_pVecData = NULL;

	// Clear map
	m_mExpression.clear();

	// Clear stack
	while ( !m_stackOper.empty() )
	{
		m_stackOper.pop();
	}

	// Clear vector
	std::vector<YCOutputItem>().swap(m_vecOutputItem);
}

void AcqArithmet::LoadExpression() throw(base::Exception)
{
	int level = 0;
	ExprInfo expInfo;

	const int VEC_SIZE = m_pVecData->size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		std::string& ref_str = (*m_pVecData)[i];

		// 非计算表达式，跳过
		if ( !IsArithmetic(ref_str) )
		{
			continue;
		}

		// 格式：[ACQ_ARITHMET:
		size_t pos_c = ref_str.find(':');
		if ( std::string::npos == pos_c )
		{
			throw base::Exception(ACQERR_ARITHMET_LOAD, "Unknown acquire arithmetic expression: [%s] (%d) [FILE:%s, LINE:%d]", ref_str.c_str(), (i+1), __FILE__, __LINE__);
		}

		size_t pos_b = ref_str.find(']');
		if ( !base::PubStr::Str2Int(ref_str.substr(pos_c+1, pos_b-pos_c-1), level) )
		{
			throw base::Exception(ACQERR_ARITHMET_LOAD, "Unknown acquire arithmetic level: [%s] (%d) [FILE:%s, LINE:%d]", ref_str.c_str(), (i+1), __FILE__, __LINE__);
		}

		expInfo.index = i;
		expInfo.expr  = base::PubStr::TrimUpperB(ref_str.substr(pos_b+1));
		m_mExpression[level] = expInfo;
	}
}

void AcqArithmet::Convert2Postorder(const std::string& expression) throw(base::Exception)
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

int AcqArithmet::GetElement(const std::string& expression, int index, std::string& element)
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

bool AcqArithmet::IsOper(const std::string& str)
{
	return ( "+" == str 
			|| "-" == str 
			|| "*" == str 
			|| "/" == str 
			|| "(" == str 
			|| ")" == str);
}

void AcqArithmet::Postorder(const std::vector<std::string>& vec_element) throw(base::Exception)
{
	if ( vec_element.empty() )
	{
		throw base::Exception(ACQERR_ARITHMET_LOAD, "The element vector is empty! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
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
			throw base::Exception(ACQERR_ARITHMET_LOAD, "SYNTAX ERROR: Parentheses do not match !!! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		}

		output_item.item_type = YCOutputItem::OIT_OPERATOR;
		output_item.vec_out.assign(1, oper);
		m_vecOutputItem.push_back(output_item);
	}
}

void AcqArithmet::DealWithOper(const std::string& oper) throw(base::Exception)
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
			throw base::Exception(ACQERR_ARITHMET_LOAD, "SYNTAX ERROR: Parentheses do not match !!! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
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

void AcqArithmet::PopStack(pFunIsCondition fun_cond)
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

std::string AcqArithmet::CalcResult() throw(base::Exception)
{
	if ( m_vecOutputItem.empty() )
	{
		throw base::Exception(ACQERR_ARITHMET_CALCULATE, "NO output item! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 从输出项队列中，取出数据项与运算符进行四则运算
	// 直至得出最终的结果
	do
	{
		CalcOnce();
	}
	while ( m_vecOutputItem.size() > 1 );

	return m_vecOutputItem[0].vec_out[0];
}

void AcqArithmet::CalcOnce() throw(base::Exception)
{
	const int VEC_SIZE = m_vecOutputItem.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YCOutputItem& ref_item = m_vecOutputItem[i];

		// 未知类型
		if ( YCOutputItem::OIT_UNKNOWN == ref_item.item_type )
		{
			throw base::Exception(ACQERR_ARITHMET_CALCULATE, "Unknown output item type! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
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
			throw base::Exception(ACQERR_ARITHMET_CALCULATE, "NO operate for operator: [%s] [FILE:%s, LINE:%d]", ref_item.vec_out[0].c_str(), __FILE__, __LINE__);
		}

		// 是否少于 2 个操作数？
		if ( i < 2 )
		{
			throw base::Exception(ACQERR_ARITHMET_CALCULATE, "Less than 2 operands: [%d] [FILE:%s, LINE:%d]", i, __FILE__, __LINE__);
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
	throw base::Exception(ACQERR_ARITHMET_CALCULATE, "The operator: NOT FOUND! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
}

AcqArithmet::pFunOperate AcqArithmet::GetOperate(const std::string& oper)
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

void AcqArithmet::OperateResult(YCOutputItem& item_left, YCOutputItem& item_right, pFunOperate pfun_oper, YCOutputItem& item_result) throw(base::Exception)
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
		TryGetDataValue(item_left);
		TryGetDataValue(item_right);

		// 此时，输出项类型只存在2种情况："常量类型"或者"数值类型"
		// 且至少有一个输出项类型为"数值类型"
		TryMatchOutputItem(item_right, item_left);
		TryMatchOutputItem(item_left, item_right);

		OperateValue(item_left, item_right, pfun_oper, item_result);
	}
}

void AcqArithmet::TryGetDataValue(YCOutputItem& item) throw(base::Exception)
{
	if ( YCOutputItem::OIT_DIM == item.item_type )
	{
		std::string& ref_out = item.vec_out[0];

		const size_t DV_PREFIX_SIZE = std::string(S_DATVAL_PREFIX).size();
		if ( ref_out.size() <= DV_PREFIX_SIZE )
		{
			throw base::Exception(ACQERR_ARITHMET_CALCULATE, "Unknown data mark: [%s] [FILE:%s, LINE:%d]", ref_out.c_str(), __FILE__, __LINE__);
		}

		if ( ref_out.substr(0, DV_PREFIX_SIZE) != S_DATVAL_PREFIX )
		{
			throw base::Exception(ACQERR_ARITHMET_CALCULATE, "Unknown data mark prefix: [%s] [FILE:%s, LINE:%d]", ref_out.c_str(), __FILE__, __LINE__);
		}

		int dv_index = 0;
		if ( !base::PubStr::Str2Int(ref_out.substr(DV_PREFIX_SIZE), dv_index) )
		{
			throw base::Exception(ACQERR_ARITHMET_CALCULATE, "Unknown data value index: [%s] [FILE:%s, LINE:%d]", ref_out.c_str(), __FILE__, __LINE__);
		}

		if ( dv_index < 1 || dv_index > (int)m_pVecData->size() )
		{
			throw base::Exception(ACQERR_ARITHMET_CALCULATE, "Invalid data value index: [%d] [FILE:%s, LINE:%d]", dv_index, __FILE__, __LINE__);
		}

		item.vec_out.assign(1, (*m_pVecData)[dv_index-1]);
		item.item_type = YCOutputItem::OIT_VALUE;
	}
}

void AcqArithmet::TryMatchOutputItem(const YCOutputItem& item_val, YCOutputItem& item_const)
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

void AcqArithmet::OperateValue(YCOutputItem& item_left, YCOutputItem& item_right, pFunOperate pfun_oper, YCOutputItem& item_result) throw(base::Exception)
{
	const int VEC_SIZE = item_left.vec_out.size();
	if ( (size_t)VEC_SIZE != item_right.vec_out.size() )
	{
		throw base::Exception(ACQERR_ARITHMET_CALCULATE, "The size of output items do not match: Left output item size [%lu], right output item size [%lu] [FILE:%s, LINE:%d]", VEC_SIZE, item_right.vec_out.size(), __FILE__, __LINE__);
	}

	std::vector<std::string> vec_val;
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		vec_val.push_back((*pfun_oper)(item_left.vec_out[i], item_right.vec_out[i]));
	}

	item_result.item_type = YCOutputItem::OIT_VALUE;
	item_result.vec_out.swap(vec_val);
}

