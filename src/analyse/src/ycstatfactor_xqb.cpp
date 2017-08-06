#include "ycstatfactor_xqb.h"
#include "anaerror.h"
#include "log.h"

YCStatFactor_XQB::YCStatFactor_XQB(const std::string& etl_day, YCTaskReq& task_req)
:YCStatFactor(etl_day, task_req)
{
}

YCStatFactor_XQB::~YCStatFactor_XQB()
{
}

void YCStatFactor_XQB::ClearOldFactors()
{
	// 清空旧数据
	if ( !m_mFactor.empty() )
	{
		m_mFactor.clear();
	}
}

void YCStatFactor_XQB::LoadOneFactor(const std::string& dim, const VEC_STRING& vec_dat) throw(base::Exception)
{
	if ( m_mFactor.find(dim) != m_mFactor.end() )
	{
		throw base::Exception(ANAERR_LOAD_FACTOR, "重复的维度因子ID：[%s] [FILE:%s, LINE:%d]", dim.c_str(), __FILE__, __LINE__);
	}

	if ( !m_mFactor[dim].Import(vec_dat) )
	{
		throw base::Exception(ANAERR_LOAD_FACTOR, "业财采集结果数据错误，无法导入因子数据：DIM=[%s], SIZE=[%lu] [FILE:%s, LINE:%d]", dim.c_str(), vec_dat.size(), __FILE__, __LINE__);
	}
}

void YCStatFactor_XQB::MakeResult(VEC3_STRING& v3_result) throw(base::Exception)
{
	VEC2_STRING vec2_result;
	VEC3_STRING vec3_result;

	m_pLog->Output("[YCStatFactor_XQB] 生成业财稽核地市详情表结果数据");
	GenerateStatResult(vec2_result);
	base::PubStr::VVVectorSwapPushBack(vec3_result, vec2_result);

	vec3_result.swap(v3_result);
}

void YCStatFactor_XQB::MakeStatInfoResult(int batch, const YCStatInfo& st_info, bool agg, VEC2_STRING& vec2_result) throw(base::Exception)
{
	YCResult_XQB ycr;
	ycr.bill_cyc = m_etlDay.substr(0, 6);			// 账期为月份：YYYYMM
	ycr.city     = m_pTaskReq->task_city;
	ycr.type     = "0";			// 类型默认值：0-固定项
	ycr.dim_id   = st_info.statdim_id;
	ycr.batch    = batch;

	VEC_STRING vec_data;
	if ( agg )	// 组合因子
	{
		YCFactor_XQB factor;
		CalcComplexFactor(st_info.stat_sql, factor);

		// 记录组合因子结果
		if ( m_mFactor.find(st_info.statdim_id) != m_mFactor.end() )
		{
			throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "重复的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", st_info.statdim_id.c_str(), __FILE__, __LINE__);
		}
		else
		{
			ycr.ImportFromFactor(factor);
			m_mFactor[st_info.statdim_id] = factor;
		}
	}
	else	// 一般因子
	{
		MAP_FACTOR::iterator m_it = m_mFactor.find(st_info.statdim_id);
		if ( m_it != m_mFactor.end() )
		{
			ycr.ImportFromFactor(m_it->second);
		}
		else
		{
			throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "不存在的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", st_info.statdim_id.c_str(), __FILE__, __LINE__);
		}
	}

	m_pLog->Output("[YCStatFactor_XQB] 因子结果：DIM=[%s], AREA=[%s], ITEM=[%s], VALUE=[%s]", ycr.dim_id.c_str(), ycr.area.c_str(), ycr.item.c_str(), ycr.value.c_str());
	ycr.Export(vec_data);
	base::PubStr::VVectorSwapPushBack(vec2_result, vec_data);
}

void YCStatFactor_XQB::CalcComplexFactor(const std::string& cmplx_fmt, YCFactor_XQB& factor) throw(base::Exception)
{
	VEC_STRING vec_fmt_first;
	VEC_STRING vec_fmt_second;

	// 格式：[ {父项目内容}; {子项目内容}; A1, A2, A3, ...|+, -, ... ]
	base::PubStr::Str2StrVector(cmplx_fmt, ";", vec_fmt_first);
	if ( vec_fmt_first.size() != 3 )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "无法识别的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fmt.c_str(), __FILE__, __LINE__);
	}

	factor.area = vec_fmt_first[0];					// 父项目内容
	factor.item = vec_fmt_first[1];					// 子项目内容

	std::string complex_result;
	MAP_FACTOR::iterator m_it;

	// 组合因子表达式：[ A1, A2, A3, ...|+, -, ... ]
	m_pLog->Output("[YCStatFactor_XQB] 组合因子表达式：%s", vec_fmt_first[2].c_str());
	base::PubStr::Str2StrVector(vec_fmt_first[2], "|", vec_fmt_first);
	if ( vec_fmt_first.size() != 2 )
	{
		// 特殊的组合因子：不含运算符，直接等于某个因子（一般因子或者组合因子）
		if ( vec_fmt_first.size() == 1 )
		{
			const std::string& REF_DIM = vec_fmt_first[0];

			if ( (m_it = m_mFactor.find(REF_DIM)) != m_mFactor.end() )
			{
				factor.value = OperateOneFactor(complex_result, "+", m_it->second.value);
				return;
			}
			else
			{
				throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", REF_DIM.c_str(), __FILE__, __LINE__);
			}
		}
		else
		{
			throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "无法识别的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fmt.c_str(), __FILE__, __LINE__);
		}
	}

	std::string fmt_dim = base::PubStr::TrimUpperB(vec_fmt_first[0]);
	std::string fmt_op  = base::PubStr::TrimUpperB(vec_fmt_first[1]);

	base::PubStr::Str2StrVector(fmt_dim, ",", vec_fmt_first);
	base::PubStr::Str2StrVector(fmt_op,  ",", vec_fmt_second);

	// 至少两个统计维度；且运算符个数比维度少一个
	const int VEC_DIM_SIZE = vec_fmt_first.size();
	if ( VEC_DIM_SIZE < 2 || (size_t)VEC_DIM_SIZE != (vec_fmt_second.size() + 1) )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不匹配的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fmt.c_str(), __FILE__, __LINE__);
	}

	// 首个因子
	const std::string& FIRST_DIM = vec_fmt_first[0];
	if ( (m_it = m_mFactor.find(FIRST_DIM)) != m_mFactor.end() )
	{
		OperateOneFactor(complex_result, "+", m_it->second.value);
	}
	else
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", FIRST_DIM.c_str(), __FILE__, __LINE__);
	}

	// 计算结果
	for ( int i = 1; i < VEC_DIM_SIZE; ++i )
	{
		std::string& ref_dim = vec_fmt_first[i];

		if ( (m_it = m_mFactor.find(ref_dim)) != m_mFactor.end() )
		{
			OperateOneFactor(complex_result, vec_fmt_second[i-1], m_it->second.value);
		}
		else
		{
			throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", ref_dim.c_str(), __FILE__, __LINE__);
		}
	}

	factor.value = complex_result;
}

