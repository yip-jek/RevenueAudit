#include "ycstatfactor_xqb.h"
#include "log.h"
#include "anaerror.h"
#include "anataskinfo.h"
#include "ycfactor_xqb.h"
#include "ycresult_xqb.h"

YCStatFactor_XQB::YCStatFactor_XQB(const std::string& etl_day, YCTaskReq& task_req, int ana_type, int item_size)
:YCStatFactor(etl_day, task_req)
,ANA_TYPE(ana_type)
,ITEM_SIZE(item_size)
{
}

YCStatFactor_XQB::~YCStatFactor_XQB()
{
	ClearOldFactors();
}

void YCStatFactor_XQB::ClearOldFactors()
{
	// 清空旧数据
	if ( !m_mFactor.empty() )
	{
		// 释放资源
		for ( MAP_FACTOR::iterator it = m_mFactor.begin(); it != m_mFactor.end(); ++it )
		{
			delete it->second;
			it->second = NULL;
		}

		m_mFactor.clear();
	}
}

void YCStatFactor_XQB::LoadOneFactor(const std::string& dim, const VEC_STRING& vec_dat) throw(base::Exception)
{
	if ( m_mFactor.find(dim) != m_mFactor.end() )
	{
		throw base::Exception(ANAERR_LOAD_FACTOR, "重复的维度因子ID：[%s] [FILE:%s, LINE:%d]", dim.c_str(), __FILE__, __LINE__);
	}

	YCFactor_XQB* pFactor = CreateFactor(dim);
	m_mFactor[dim] = pFactor;

	if ( !pFactor->Import(vec_dat) )
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
	const YCResult_XQB::RESULT_FACTOR_TYPE RFACTOR_TYPE = (ANA_TYPE == AnalyseRule::ANATYPE_YCXQB_GD ? YCResult_XQB::RFT_XQB_GD : YCResult_XQB::RFT_XQB_YCW);

	YCResult_XQB ycr(RFACTOR_TYPE, ITEM_SIZE);
	ycr.bill_cyc = m_etlDay.substr(0, 6);			// 账期为月份：YYYYMM
	ycr.city     = m_pTaskReq->task_city;
	ycr.type     = "0";			// 类型默认值：0-固定项
	ycr.batch    = batch;

	VEC_STRING vec_data;
	if ( agg )	// 组合因子
	{
		// 记录组合因子结果
		if ( m_mFactor.find(st_info.statdim_id) != m_mFactor.end() )
		{
			throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "重复的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", st_info.statdim_id.c_str(), __FILE__, __LINE__);
		}

		YCFactor_XQB* pFactor = CreateFactor(st_info.statdim_id);
		m_mFactor[st_info.statdim_id] = pFactor;

		CalcComplexFactor(st_info.stat_sql, pFactor);
		ycr.ImportFromFactor(pFactor);
	}
	else	// 一般因子
	{
		MAP_FACTOR::iterator m_it = m_mFactor.find(st_info.statdim_id);
		if ( m_it == m_mFactor.end() )
		{
			throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "不存在的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", st_info.statdim_id.c_str(), __FILE__, __LINE__);
		}

		ycr.ImportFromFactor(m_it->second);
	}

	ycr.Export(vec_data);
	m_pLog->Output("[YCStatFactor_XQB] 因子结果：%s", ycr.LogPrintInfo().c_str());

	base::PubStr::VVectorSwapPushBack(vec2_result, vec_data);
}

YCFactor_XQB* YCStatFactor_XQB::CreateFactor(const std::string& dim)
{
	YCFactor_XQB* pFactor = NULL;

	switch ( ANA_TYPE )
	{
	case AnalyseRule::ANATYPE_YCXQB_GD:				// 业财详情表（省）稽核类型
		pFactor = new YCFactor_XQB_GD(ITEM_SIZE);
		break;
	case AnalyseRule::ANATYPE_YCXQB_CW:				// 业财详情表（财务侧）稽核类型
	case AnalyseRule::ANATYPE_YCXQB_YW:				// 业财详情表（业务侧）稽核类型
		pFactor = new YCFactor_XQB_YCW(ITEM_SIZE);
		break;
	case AnalyseRule::ANATYPE_YCHDB:				// 业财核对表稽核类型
	default:
		throw base::Exception(ANAERR_CREATE_FACTOR, "Create factor failed! Unsupported ANALYSE_TYPE: [%d] [FILE:%s, LINE:%d]", ANA_TYPE, __FILE__, __LINE__);
	}

	if ( NULL == pFactor )
	{
		throw base::Exception(ANAERR_CREATE_FACTOR, "Create factor failed: Operator new YCFactor_XQB failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	pFactor->SetDimID(dim);
	return pFactor;
}

void YCStatFactor_XQB::CalcComplexFactor(const std::string& cmplx_fmt, YCFactor_XQB* p_factor) throw(base::Exception)
{
	if ( NULL == p_factor )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "Invalid factor: YCFactor_XQB pointer is blank! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	VEC_STRING vec_fmt_first;
	VEC_STRING vec_fmt_second;

	// 格式：[ {父项目内容}; {子项目内容}; A1, A2, A3, ...|+, -, ... ]
	const size_t AREA_ITEM_SIZE = p_factor->GetAreaItemSize();
	base::PubStr::Str2StrVector(cmplx_fmt, ";", vec_fmt_first);
	if ( vec_fmt_first.size() != (AREA_ITEM_SIZE + 1) )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "无法识别的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fmt.c_str(), __FILE__, __LINE__);
	}

	// 父项目内容
	p_factor->SetArea(vec_fmt_first[0]);

	// 子项目内容
	if ( !p_factor->ImportItems(VEC_STRING(vec_fmt_first.begin()+1, vec_fmt_first.begin()+AREA_ITEM_SIZE)) )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "Import items of complex factor failed: DIM=[%s], COMPLEX_FMT=[%s] [FILE:%s, LINE:%d]", p_factor->GetDimID().c_str(), cmplx_fmt.c_str(), __FILE__, __LINE__);
	}

	const int FACTOR_VALUE_SIZE = p_factor->GetValueSize();
	VEC_STRING vec_result(FACTOR_VALUE_SIZE, "");

	// 组合因子表达式：[ A1, A2, A3, ...|+, -, ... ]
	m_pLog->Output("[YCStatFactor_XQB] 组合因子表达式：%s", vec_fmt_first[AREA_ITEM_SIZE].c_str());
	base::PubStr::Str2StrVector(vec_fmt_first[2], "|", vec_fmt_first);
	if ( vec_fmt_first.size() != 2 )
	{
		if ( vec_fmt_first.size() != 1 )
		{
			throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "无法识别的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fmt.c_str(), __FILE__, __LINE__);
		}

		// 特殊的组合因子：不含运算符，直接等于某个因子（一般因子或者组合因子）
		CalcOneFactor(vec_result, "+", vec_fmt_first[0]);
		return;
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
	CalcOneFactor(vec_result, "+", vec_fmt_first[0]);

	// 计算结果
	for ( int i = 1; i < VEC_DIM_SIZE; ++i )
	{
		CalcOneFactor(vec_result, vec_fmt_second[i-1], vec_fmt_first[i]);
	}

	if ( p_factor->ImportValue(vec_result) )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "Import value of complex factor failed: DIM=[%s], COMPLEX_FMT=[%s] [FILE:%s, LINE:%d]", p_factor->GetDimID().c_str(), cmplx_fmt.c_str(), __FILE__, __LINE__);
	}
}

void YCStatFactor_XQB::CalcOneFactor(VEC_STRING& vec_result, const std::string& op, const std::string& dim) throw(base::Exception)
{
	MAP_FACTOR::iterator m_it = m_mFactor.find(dim);
	if ( m_it == m_mFactor.end() )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", dim.c_str(), __FILE__, __LINE__);
	}

	const int     VEC_SIZE = vec_result.size();
	YCFactor_XQB* pFactor  = m_it->second;

	if ( pFactor->GetValueSize() != VEC_SIZE )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "业财稽核维度因子：DIM=[%s], VALUE_SIZE=[%d], 与结果集：RESULT_SIZE=[%d] 不一致！[FILE:%s, LINE:%d]", dim.c_str(), pFactor->GetValueSize(), VEC_SIZE, __FILE__, __LINE__);
	}

	VEC_STRING vec_value;
	pFactor->ExportValue(vec_value);

	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		OperateOneFactor(vec_result[i], op, vec_value[i]);
	}
}

