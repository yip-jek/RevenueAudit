#include "ycstatfactor.h"
#include "anaerror.h"
#include "log.h"

const char* const YCStatFactor::S_TOP_PRIORITY = "NN";					// 最高优先级 (差异汇总)
const char* const YCStatFactor::S_ARITHMETIC   = "ARITHMETIC";			// 四则运算标识

YCStatFactor::YCStatFactor(const std::string& etl_day, YCTaskReq& task_req)
:m_pLog(base::Log::Instance())
,m_etlDay(etl_day)
,m_refTaskReq(task_req)
,m_statArithmet(this)
{
}

YCStatFactor::~YCStatFactor()
{
	base::Log::Release();
}

std::string YCStatFactor::GetStatID() const
{
	return m_statID;
}

std::string YCStatFactor::GetStatReport() const
{
	return m_statReport;
}

void YCStatFactor::LoadStatInfo(VEC_STATINFO& vec_statinfo) throw(base::Exception)
{
	m_statID.clear();
	m_statReport.clear();
	m_mvStatInfo.clear();

	const int VEC_SIZE = vec_statinfo.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YCStatInfo& ref_si = vec_statinfo[i];

		base::PubStr::TrimUpper(ref_si.stat_priority);
		if ( ref_si.stat_priority != S_TOP_PRIORITY )
		{
			int level = 0;
			if ( !base::PubStr::Str2Int(ref_si.stat_priority, level) )
			{
				throw base::Exception(ANAERR_LOAD_STAT_INFO, "未知的因子规则优先级：%s [FILE:%s, LINE:%d]", ref_si.LogPrintInfo().c_str(), __FILE__, __LINE__);
			}
			if ( level < 0 )
			{
				throw base::Exception(ANAERR_LOAD_STAT_INFO, "无效的因子规则优先级：%s [FILE:%s, LINE:%d]", ref_si.LogPrintInfo().c_str(), __FILE__, __LINE__);
			}

			m_mvStatInfo[level].push_back(ref_si);
		}
	}

	if ( m_mvStatInfo.find(0) == m_mvStatInfo.end() )
	{
		throw base::Exception(ANAERR_LOAD_STAT_INFO, "缺少一般规则因子信息！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_statID     = m_mvStatInfo[0][0].stat_id;
	m_statReport = m_mvStatInfo[0][0].stat_report;
	m_pLog->Output("[YCStatFactor] 载入规则因子信息成功.");
}

int YCStatFactor::LoadFactors(const VEC3_STRING& v3_data) throw(base::Exception)
{
	ReleaseFactors();

	int counter = 0;
	const int VEC3_SIZE = v3_data.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		const VEC2_STRING& ref_vec2 = v3_data[i];

		const int VEC2_SIZE = ref_vec2.size();
		for ( int j = 0; j < VEC2_SIZE; ++j )
		{
			const VEC_STRING& ref_vec = ref_vec2[j];

			if ( ref_vec.size() > 1 )
			{
				// 维度ID 在第一列
				LoadOneFactor(ref_vec[0], VEC_STRING(ref_vec.begin()+1, ref_vec.end()));
			}
			else
			{
				throw base::Exception(ANAERR_LOAD_FACTOR, "业财采集结果数据错误，无效数据 SIZE: [%lu] [FILE:%s, LINE:%d]", ref_vec.size(), __FILE__, __LINE__);
			}
		}

		++counter;
	}

	m_pLog->Output("[YCStatFactor] 载入因子对成功.");
	return counter;
}

void YCStatFactor::GenerateStatResult(VEC2_STRING& v2_result) throw(base::Exception)
{
	VEC2_STRING vec2_result;

	// 生成一般因子与组合因子的结果数据
	bool        agg = false;
	std::string factor_type;
	for ( MAP_VEC_STATINFO::iterator m_it = m_mvStatInfo.begin(); m_it != m_mvStatInfo.end(); ++m_it )
	{
		if ( m_it->first > 0 )		// 组合（合计）因子
		{
			agg         = true;
			factor_type = "组合（合计）因子";
		}
		else	// 一般因子
		{
			agg         = false;
			factor_type = "一般因子";
		}

		const int VEC_SIZE = m_it->second.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			YCStatInfo& ref_si = (m_it->second)[i];

			m_pLog->Output("[YCStatFactor] 统计因子类型：%s", factor_type.c_str());
			m_pLog->Output("[YCStatFactor] 生成统计因子结果数据：%s", ref_si.LogPrintInfo().c_str());
			MakeStatInfoResult(m_refTaskReq.task_batch, ref_si, agg, vec2_result);
		}
	}

	vec2_result.swap(v2_result);
}

double YCStatFactor::OperateOneFactor(double left, const std::string& op, const std::string& factor) throw(base::Exception)
{
	double d_factor = 0.0;
	if ( factor.empty() || !base::PubStr::Str2Double(factor, d_factor) )
	{
		throw base::Exception(ANAERR_OPERATE_ONE_FACTOR, "非有效因子值：%s [FILE:%s, LINE:%d]", factor.c_str(), __FILE__, __LINE__);
	}

	if ( "+" == op )
	{
		left += d_factor;
	}
	else if ( "-" == op )
	{
		left -= d_factor;
	}
	else if ( "*" == op )
	{
		left *= d_factor;
	}
	else if ( "/" == op )
	{
		left /= d_factor;
	}
	else
	{
		throw base::Exception(ANAERR_OPERATE_ONE_FACTOR, "无法识别的因子运算符：%s [FILE:%s, LINE:%d]", op.c_str(), __FILE__, __LINE__);
	}

	return left;
}

std::string YCStatFactor::ConvertResultType(double result)
{
	// 加上辅助值(0.000001)，
	// 以避免小数点后第三位是5但“四舍五入”不进位的问题
	result += 1e-6;

	// Double -> String，保留2位小数
	return base::PubStr::Double2FormatStr(result);
}

bool YCStatFactor::IsArithmetic(const std::string& expr, std::string& arith_exp)
{
	// 四则运算标识：表达式首部为"[ARITHMETIC]"
	std::string expr_head = base::PubStr::TrimB(expr);

	// 表达式是否满足最小长度？
	if ( expr_head.size() <= (std::string(S_ARITHMETIC).size() + 2) )
	{
		return false;
	}

	// 不是以'['开始？
	if ( expr_head[0] != '[' )
	{
		return false;
	}

	// 是否存在']'？
	const size_t POS_END = expr_head.find(']');
	if ( POS_END == std::string::npos )
	{
		return false;
	}

	// 是否为四则运算标识？
	if ( base::PubStr::TrimUpperB(expr_head.substr(1, POS_END-1)) != S_ARITHMETIC )
	{
		return false;
	}

	arith_exp = base::PubStr::TrimB(expr_head.substr(POS_END+1));
	return true;
}

