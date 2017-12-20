#include "ycstatfactor_xqb.h"
#include "log.h"
#include "anaerror.h"
#include "anataskinfo.h"
#include "ycfactor_xqb.h"
#include "ycresult_xqb.h"

YCStatFactor_XQB::YCStatFactor_XQB(const std::string& etl_day, YCTaskReq& task_req, int ana_type, int item_size, int val_size)
:YCStatFactor(etl_day, task_req)
,ANA_TYPE(ana_type)
,ITEM_SIZE(item_size)
,VAL_SIZE(val_size)
{
}

YCStatFactor_XQB::~YCStatFactor_XQB()
{
	ReleaseFactors();
}

void YCStatFactor_XQB::ReleaseFactors()
{
	// 清空旧数据
	if ( !m_mFactor.empty() )
	{
		m_mFactor.clear();
	}
}

void YCStatFactor_XQB::LoadOneFactor(const std::string& dim, const VEC_STRING& vec_dat) throw(base::Exception)
{
	YCStatInfo yc_si;
	yc_si.SetDim(dim);
	const std::string SI_DIM = yc_si.GetDim();

	if ( m_mFactor.find(SI_DIM) != m_mFactor.end() )
	{
		throw base::Exception(ANAERR_LOAD_FACTOR, "重复的维度因子ID：[%s] [FILE:%s, LINE:%d]", SI_DIM.c_str(), __FILE__, __LINE__);
	}

	// 在首位补上 DIM
	VEC_STRING v_data = vec_dat;
	v_data.insert(v_data.begin(), SI_DIM);

	YCFactor_XQB factor(ITEM_SIZE, VAL_SIZE);
	if ( !factor.Import(v_data) )
	{
		throw base::Exception(ANAERR_LOAD_FACTOR, "业财采集结果数据错误，无法导入因子数据：DIM=[%s], SIZE=[%lu] [FILE:%s, LINE:%d]", SI_DIM.c_str(), v_data.size(), __FILE__, __LINE__);
	}

	m_mFactor.insert(PAIR_FACTOR(SI_DIM, factor));
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

void YCStatFactor_XQB::GetDimFactorValue(const std::string& dim, VEC_STRING& vec_val) throw(base::Exception)
{
	MAP_STRING::iterator m_it = m_mFactor.find(dim);
	if ( m_it != m_mFactor.end() )
	{
		m_it->ExportValue(vec_val);
	}
	else
	{
		throw base::Exception(ANAERR_GET_DIM_FACTOR_VAL, "无法匹配到的维度ID：%s [FILE:%s, LINE:%d]", dim.c_str(), __FILE__, __LINE__);
	}
}

void YCStatFactor_XQB::MakeStatInfoResult(int batch, const YCStatInfo& st_info, bool agg, VEC2_STRING& vec2_result) throw(base::Exception)
{
	YCResult_XQB ycr(ANA_TYPE, ITEM_SIZE, VAL_SIZE);
	ycr.bill_cyc = m_etlDay.substr(0, 6);			// 账期为月份：YYYYMM
	ycr.city     = GetResultCity();
	ycr.type     = "0";			// 类型默认值：0-固定项
	ycr.batch    = batch;

	const std::string DIM = st_info.GetDim();
	YCFactor_XQB factor(ITEM_SIZE, VAL_SIZE);
	VEC_STRING   vec_data;

	if ( agg )	// 组合因子
	{
		if ( m_mFactor.find(DIM) != m_mFactor.end() )
		{
			throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "重复的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", DIM.c_str(), __FILE__, __LINE__);
		}

		factor.SetDimID(DIM);
		CalcComplexFactor(st_info.stat_sql, factor);

		// 记录组合因子结果
		m_mFactor.insert(PAIR_FACTOR(DIM, factor));
	}
	else	// 一般因子
	{
		MAP_FACTOR::iterator m_it = m_mFactor.find(DIM);
		if ( m_it == m_mFactor.end() )
		{
			throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "不存在的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", DIM.c_str(), __FILE__, __LINE__);
		}

		factor = m_it->second;
	}

	ycr.ImportFactor(factor);
	m_pLog->Output("[YCStatFactor_XQB] 因子结果：%s", ycr.LogPrintInfo().c_str());

	// 虚因子只参与计算，不入库！
	if ( st_info.IsVirtual() )
	{
		m_pLog->Output("[YCStatFactor_XQB] 虚因子(不入库)：%s", st_info.LogPrintInfo().c_str());
	}
	else
	{
		ycr.Export(vec_data);
		base::PubStr::VVectorSwapPushBack(vec2_result, vec_data);
	}
}

std::string YCStatFactor_XQB::GetResultCity()
{
	if ( AnalyseRule::ANATYPE_YCXQB_GD == ANA_TYPE )	// 详情表（省）
	{
		return "GD";
	}
	else	// 详情表（业务侧、财务侧）
	{
		return m_refTaskReq.task_city;
	}
}

void YCStatFactor_XQB::CalcComplexFactor(const std::string& cmplx_fmt, YCFactor_XQB& factor) throw(base::Exception)
{
	VEC_STRING vec_fmt_first;
	VEC_STRING vec_fmt_second;

	// 格式：[ {父项目内容}; {子项目内容}; A1, A2, A3, ...|+, -, ... ]
	const size_t AREA_ITEM_SIZE = factor.GetAreaItemSize();
	base::PubStr::Str2StrVector(cmplx_fmt, ";", vec_fmt_first);
	if ( vec_fmt_first.size() != (AREA_ITEM_SIZE + 1) )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "无法识别的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fmt.c_str(), __FILE__, __LINE__);
	}

	// 父项目内容
	factor.SetArea(vec_fmt_first[0]);

	// 子项目内容
	if ( !factor.ImportItems(VEC_STRING(vec_fmt_first.begin()+1, vec_fmt_first.begin()+AREA_ITEM_SIZE)) )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "Import items of complex factor failed: DIM=[%s], COMPLEX_FMT=[%s] [FILE:%s, LINE:%d]", factor.GetDimID().c_str(), cmplx_fmt.c_str(), __FILE__, __LINE__);
	}

	const int FACTOR_VALUE_SIZE = factor.GetValueSize();
	VEC_DOUBLE vec_result(FACTOR_VALUE_SIZE, 0.0);

	// 组合因子表达式：[ A1, A2, A3, ...|+, -, ... ]
	m_pLog->Output("[YCStatFactor_XQB] 组合因子表达式：%s", vec_fmt_first[AREA_ITEM_SIZE].c_str());
	base::PubStr::Str2StrVector(vec_fmt_first[AREA_ITEM_SIZE], "|", vec_fmt_first);

	const int VEC_FMT_SIZE = vec_fmt_first.size();
	if ( VEC_FMT_SIZE == 1 )	// 特殊的组合因子：不含运算符，直接等于某个因子（一般因子或者组合因子）
	{
		CalcOneFactor(vec_result, "+", vec_fmt_first[0]);
	}
	else if ( VEC_FMT_SIZE == 2 )	// 正常的组合因子：左边维度ID集，右边操作符集
	{
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

		// 计算结果：首个因子
		CalcOneFactor(vec_result, "+", vec_fmt_first[0]);

		// 计算结果：剩余因子
		for ( int i = 1; i < VEC_DIM_SIZE; ++i )
		{
			CalcOneFactor(vec_result, vec_fmt_second[i-1], vec_fmt_first[i]);
		}
	}
	else
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "无法识别的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fmt.c_str(), __FILE__, __LINE__);
	}

	VEC_STRING vs_result;
	ConvertVectorResultType(vec_result, vs_result);
	if ( !factor.ImportValue(vs_result) )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "Import value of complex factor failed: DIM=[%s], COMPLEX_FMT=[%s] [FILE:%s, LINE:%d]", factor.GetDimID().c_str(), cmplx_fmt.c_str(), __FILE__, __LINE__);
	}
}

void YCStatFactor_XQB::CalcOneFactor(VEC_DOUBLE& vec_result, const std::string& op, const std::string& dim) throw(base::Exception)
{
	VEC_STRING vec_value;
	double     dou      = 0.0;
	const int  VEC_SIZE = vec_result.size();

	MAP_FACTOR::iterator m_it = m_mFactor.find(dim);
	if ( m_it != m_mFactor.end() )
	{
		YCFactor_XQB& ref_factor = m_it->second;
		if ( ref_factor.GetValueSize() != VEC_SIZE )
		{
			throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "业财稽核维度因子：DIM=[%s], VALUE_SIZE=[%d], 与结果集：RESULT_SIZE=[%d] 不一致！[FILE:%s, LINE:%d]", dim.c_str(), ref_factor.GetValueSize(), VEC_SIZE, __FILE__, __LINE__);
		}

		ref_factor.ExportValue(vec_value);
	}
	else if ( base::PubStr::Str2Double(dim, dou) )
	{
		m_pLog->Output("[YCStatFactor_XQB] 常量维度：[%s]", dim.c_str());
		vec_value.assign(VEC_SIZE, dim);
	}
	else
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", dim.c_str(), __FILE__, __LINE__);
	}

	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		vec_result[i] = OperateOneFactor(vec_result[i], op, vec_value[i]);
	}
}

void YCStatFactor_XQB::ConvertVectorResultType(const VEC_DOUBLE& vec_dou, VEC_STRING& vec_result)
{
	VEC_STRING vec_res;

	const int VEC_SIZE = vec_dou.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		vec_res.push_back(ConvertResultType(vec_dou[i]));
	}

	vec_res.swap(vec_result);
}

