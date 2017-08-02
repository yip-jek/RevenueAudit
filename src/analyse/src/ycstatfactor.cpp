#include "ycstatfactor.h"
#include "anaerror.h"
#include "pubstr.h"
#include "log.h"

const char* const YCStatFactor::S_TOP_PRIORITY  = "NN";			// 最高优先级 (差异汇总)


YCStatFactor::YCStatFactor()
:m_pLog(base::Log::Instance())
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

	if ( !m_mvStatInfo.empty() )
	{
		m_mvStatInfo.clear();
	}
	if ( !m_vTopStatInfo.empty() )
	{
		VEC_STATINFO().swap(m_vTopStatInfo);
	}

	const int VEC_SIZE = vec_statinfo.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YCStatInfo& ref_si = vec_statinfo[i];
		base::PubStr::TrimUpper(ref_si.statdim_id);

		// 是否为分类因子：维度 ID 包含问号
		ref_si.category = (ref_si.statdim_id.find('?') != std::string::npos);

		if ( S_TOP_PRIORITY == base::PubStr::TrimUpperB(ref_si.stat_priority) )
		{
			m_vTopStatInfo.push_back(ref_si);
		}
		else
		{
			int level = 0;
			if ( !base::PubStr::Str2Int(ref_si.stat_priority, level) )
			{
				throw base::Exception(ANAERR_LOAD_STAT_INFO, "未知的因子规则优先级：[%s] (STAT_ID=[%s], STAT_NAME=[%s], DIM_ID=[%s], STAT_REPORT=[%s]) [FILE:%s, LINE:%d]", ref_si.stat_priority.c_str(), ref_si.stat_id.c_str(), ref_si.stat_name.c_str(), ref_si.statdim_id.c_str(), ref_si.stat_report.c_str(), __FILE__, __LINE__);
			}
			if ( level < 0 )
			{
				throw base::Exception(ANAERR_LOAD_STAT_INFO, "无效的因子规则优先级：[%s] (STAT_ID=[%s], STAT_NAME=[%s], DIM_ID=[%s], STAT_REPORT=[%s]) [FILE:%s, LINE:%d]", ref_si.stat_priority.c_str(), ref_si.stat_id.c_str(), ref_si.stat_name.c_str(), ref_si.statdim_id.c_str(), ref_si.stat_report.c_str(), __FILE__, __LINE__);
			}

			m_mvStatInfo[level].push_back(ref_si);
		}
	}

	if ( m_mvStatInfo.find(0) != m_mvStatInfo.end() )
	{
		m_statID     = m_mvStatInfo[0][0].stat_id;
		m_statReport = m_mvStatInfo[0][0].stat_report;
	}
	else
	{
		throw base::Exception(ANAERR_LOAD_STAT_INFO, "缺少一般规则因子信息！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pLog->Output("[YCStatFactor] 载入规则因子信息成功.");
}

int YCStatFactor::LoadFactor(std::vector<std::vector<std::vector<std::string> > >& v3_data) throw(base::Exception)
{
	if ( !m_mFactor.empty() )
	{
		m_mFactor.clear();
	}

	int              counter = 0;
	std::string      dim_id;
	YCCategoryFactor ctgFactor;

	const int VEC3_SIZE = v3_data.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		std::vector<std::vector<std::string> >& ref_vec2 = v3_data[i];

		const int VEC2_SIZE = ref_vec2.size();
		for ( int j = 0; j < VEC2_SIZE; ++j )
		{
			std::vector<std::string>& ref_vec = ref_vec2[j];

			const int VEC_SIZE = ref_vec.size();
			if ( VEC_SIZE > 0 )
			{
				// 维度ID 在第一列
				dim_id = base::PubStr::TrimUpperB(ref_vec[0]);

				if ( IsCategoryDim(dim_id) )	// 分类因子
				{
					if ( S_CATEGORY_COLUMN_SIZE == VEC_SIZE )
					{
						ctgFactor.dim_id = dim_id;
						ctgFactor.item   = base::PubStr::TrimB(ref_vec[1]);
						ctgFactor.value  = base::PubStr::StringDoubleFormat(ref_vec[2]);
						m_mvCategoryFactor[dim_id].push_back(ctgFactor);
					}
					else
					{
						throw base::Exception(ANAERR_LOAD_FACTOR, "业财采集结果数据错误，分类因子数据 SIZE 不匹配：[%lu] [FILE:%s, LINE:%d]", VEC_SIZE, __FILE__, __LINE__);
					}
				}
				else	// 一般因子
				{
					if ( m_mFactor.find(dim_id) != m_mFactor.end() )
					{
						throw base::Exception(ANAERR_LOAD_FACTOR, "重复的维度因子ID：[%s] [FILE:%s, LINE:%d]", dim_id.c_str(), __FILE__, __LINE__);
					}

					m_mFactor[dim_id] = ref_vec;
				}
			}
			else
			{
				throw base::Exception(ANAERR_LOAD_FACTOR, "业财采集结果数据错误，无效数据 SIZE: [%lu] [FILE:%s, LINE:%d]", VEC_SIZE, __FILE__, __LINE__);
			}

			++counter;
		}
	}

	m_pLog->Output("[YCStatFactor] 载入因子对成功.");
	return counter;
}

void YCStatFactor::GenerateResult(int batch, const std::string& city, std::vector<std::vector<std::string> >& v2_result) throw(base::Exception)
{
	std::vector<std::vector<std::string> > vec2_result;

	// 生成一般因子与组合因子的结果数据
	for ( MAP_VEC_STATINFO::iterator m_it = m_mvStatInfo.begin(); m_it != m_mvStatInfo.end(); ++m_it )
	{
		const int VEC_SIZE = m_it->second.size();

		if ( m_it->first > 0 )		// 组合（合计）因子
		{
			for ( int i = 0; i < VEC_SIZE; ++i )
			{
				YCStatInfo& ref_si = (m_it->second)[i];

				m_pLog->Output("[YCStatFactor] 统计因子类型：组合（合计）因子");
				m_pLog->Output("[YCStatFactor] 生成统计因子结果数据：STAT_ID:%s, STATDIM_ID:%s, STAT_PRIORITY:%s", ref_si.stat_id.c_str(), ref_si.statdim_id.c_str(), ref_si.stat_priority.c_str());
				MakeStatInfoResult(batch, city, ref_si, true, vec2_result);
			}
		}
		else	// 一般因子
		{
			for ( int i = 0; i < VEC_SIZE; ++i )
			{
				YCStatInfo& ref_si = (m_it->second)[i];

				m_pLog->Output("[YCStatFactor] 统计因子类型：一般因子");
				m_pLog->Output("[YCStatFactor] 生成统计因子结果数据：STAT_ID:%s, STATDIM_ID:%s, STAT_PRIORITY:%s", ref_si.stat_id.c_str(), ref_si.statdim_id.c_str(), ref_si.stat_priority.c_str());
				MakeStatInfoResult(batch, city, ref_si, false, vec2_result);
			}
		}
	}

	vec2_result.swap(v2_result);
}

void YCStatFactor::GenerateDiffSummaryResult(const std::string& city, std::vector<std::vector<std::string> >& v2_result) throw(base::Exception)
{
	std::vector<std::vector<std::string> > vec2_result;

	// 生成汇总因子的结果数据
	const int VEC_TOP_SIZE = m_vTopStatInfo.size();
	for ( int n = 0; n < VEC_TOP_SIZE; ++n )
	{
		YCStatInfo& ref_si = m_vTopStatInfo[n];

		m_pLog->Output("[YCStatFactor] 统计因子类型：汇总因子");
		m_pLog->Output("[YCStatFactor] 生成汇总因子结果数据：STAT_ID:%s, STATDIM_ID:%s, STAT_PRIORITY:%s", ref_si.stat_id.c_str(), ref_si.statdim_id.c_str(), ref_si.stat_priority.c_str());

		// 汇总因子批次锁定为 0
		MakeStatInfoResult(0, city, ref_si, true, vec2_result);
	}

	vec2_result.swap(v2_result);
}

std::string YCStatFactor::CalcComplexFactor(const std::string& cmplx_fctr_fmt) throw(base::Exception)
{
	m_pLog->Output("[YCStatFactor] 组合因子表达式：%s", cmplx_fctr_fmt.c_str());

	std::string cmplx_fctr_result;
	MAP_FACTOR::iterator m_it;

	// 组合因子格式：[ A1, A2, A3, ...|+, -, ... ]
	std::vector<std::string> vec_fmt_left;
	base::PubStr::Str2StrVector(cmplx_fctr_fmt, "|", vec_fmt_left);
	if ( vec_fmt_left.size() != 2 )
	{
		if ( vec_fmt_left.size() == 1 )		// 特殊的组合因子：不含运算符，直接等于某个因子（一般因子或者组合因子）
		{
			std::string& ref_fmt = vec_fmt_left[0];
			if ( ref_fmt.find('?') != std::string::npos )	// 因子格式（参考）：{A?_B?}
			{
				return CalcCategoryFactor(ref_fmt);
			}
			else if ( (m_it = m_mFactor.find(ref_fmt)) != m_mFactor.end() )
			{
				OperateOneFactor(cmplx_fctr_result, "+", m_it->second.GetVal());
				return cmplx_fctr_result;
			}
			else
			{
				throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", vec_fmt_left[0].c_str(), __FILE__, __LINE__);
			}
		}
		else
		{
			throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "无法识别的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fctr_fmt.c_str(), __FILE__, __LINE__);
		}
	}

	std::string yc_dims = base::PubStr::TrimUpperB(vec_fmt_left[0]);
	std::string yc_oper = base::PubStr::TrimUpperB(vec_fmt_left[1]);

	std::vector<std::string> vec_fmt_right;
	base::PubStr::Str2StrVector(yc_dims, ",", vec_fmt_left);
	base::PubStr::Str2StrVector(yc_oper, ",", vec_fmt_right);

	// 至少两个统计维度；且运算符个数比维度少一个
	const int VEC_DIM_SIZE = vec_fmt_left.size();
	if ( VEC_DIM_SIZE < 2 || (size_t)VEC_DIM_SIZE != (vec_fmt_right.size() + 1) )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不匹配的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fctr_fmt.c_str(), __FILE__, __LINE__);
	}

	// 首个因子
	std::string& ref_first = vec_fmt_left[0];
	if ( ref_first.find('?') != std::string::npos )		// 因子格式（参考）：{A?_B?}
	{
		OperateOneFactor(cmplx_fctr_result, "+", CalcCategoryFactor(ref_first));
	}
	else if ( (m_it = m_mFactor.find(ref_first)) != m_mFactor.end() )
	{
		OperateOneFactor(cmplx_fctr_result, "+", m_it->second.GetVal());
	}
	else
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", ref_first.c_str(), __FILE__, __LINE__);
	}
	
	// 计算结果
	for ( int i = 1; i < VEC_DIM_SIZE; ++i )
	{
		std::string& ref_fmt = vec_fmt_left[i];
		if ( ref_fmt.find('?') != std::string::npos )	// 因子格式（参考）：{A?_B?}
		{
			OperateOneFactor(cmplx_fctr_result, "+", CalcCategoryFactor(ref_fmt));
		}
		else if ( (m_it = m_mFactor.find(ref_fmt)) != m_mFactor.end() )
		{
			OperateOneFactor(cmplx_fctr_result, vec_fmt_right[i-1], m_it->second.GetVal());
		}
		else
		{
			throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", ref_fmt.c_str(), __FILE__, __LINE__);
		}
	}

	return cmplx_fctr_result;
}

std::string YCStatFactor::CalcCategoryFactor(const std::string& ctg_fmt) throw(base::Exception)
{
	std::string fmt = base::PubStr::TrimUpperB(ctg_fmt);
	m_pLog->Output("[YCStatFactor] 组合分类因子表达式：%s", fmt.c_str());

	const int FMT_SIZE = fmt.size();
	if ( FMT_SIZE > 3 && '{' == fmt[0] && '}' == fmt[FMT_SIZE-1] )
	{
		// 去除外大括号
		fmt = fmt.substr(1, FMT_SIZE-2);
		base::PubStr::TrimUpper(fmt);

		if ( fmt.find(',') != std::string::npos || fmt.find('\x20') != std::string::npos )
		{
			throw base::Exception(ANAERR_CALC_CATEGORY_FACTOR, "无法识别的组合分类因子表达式：%s [FILE:%s, LINE:%d]", ctg_fmt.c_str(), __FILE__, __LINE__);
		}

		int ctg_index = 1;
		std::string ctg_result;
		std::string factor;
		while ( GetCategoryFactorValue(fmt, ctg_index, factor) )
		{
			OperateOneFactor(ctg_result, "+", factor);

			++ctg_index;
		}

		m_pLog->Output("[YCStatFactor] 组合分类因子表达式 %s 共包含 [%d] 个分类因子", ctg_fmt.c_str(), (ctg_index-1));
		return ctg_result;
	}
	else
	{
		throw base::Exception(ANAERR_CALC_CATEGORY_FACTOR, "无法识别的组合分类因子表达式：%s [FILE:%s, LINE:%d]", ctg_fmt.c_str(), __FILE__, __LINE__);
	}
}

std::string YCStatFactor::ExtendCategoryDim(const std::string& dim, int index)
{
	std::string       new_dim   = dim;
	const std::string STR_INDEX = base::PubStr::Int2Str(index);

	// 将问号（?）替换为 index
	size_t pos = 0;
	while ( (pos = new_dim.find('?')) != std::string::npos )
	{
		new_dim.replace(pos, 1, STR_INDEX);
	}

	return new_dim;
}

bool YCStatFactor::GetCategoryFactorValue(const std::string& ctg_fmt, int index, std::string& val)
{
	MAP_FACTOR::iterator m_it = m_mFactor.find(ExtendCategoryDim(ctg_fmt, index));
	if ( m_it != m_mFactor.end() )
	{
		val = m_it->second.GetVal();
		return true;
	}

	return false;
}

void YCStatFactor::OperateOneFactor(std::string& result, const std::string& op, const std::string& factor) throw(base::Exception)
{
	double dou_res = 0.0;
	if ( !result.empty() && !base::PubStr::Str2Double(result, dou_res) )
	{
		throw base::Exception(ANAERR_OPERATE_ONE_FACTOR, "结果值无法转化为精度型：%s [FILE:%s, LINE:%d]", result.c_str(), __FILE__, __LINE__);
	}

	double dou_fctr = 0.0;
	if ( factor.empty() || !base::PubStr::Str2Double(factor, dou_fctr) )
	{
		throw base::Exception(ANAERR_OPERATE_ONE_FACTOR, "非有效因子值：%s [FILE:%s, LINE:%d]", factor.c_str(), __FILE__, __LINE__);
	}

	if ( "+" == op )
	{
		dou_res += dou_fctr;
	}
	else if ( "-" == op )
	{
		dou_res -= dou_fctr;
	}
	else if ( "*" == op )
	{
		dou_res *= dou_fctr;
	}
	else if ( "/" == op )
	{
		dou_res /= dou_fctr;
	}
	else
	{
		throw base::Exception(ANAERR_OPERATE_ONE_FACTOR, "无法识别的因子运算符：%s [FILE:%s, LINE:%d]", op.c_str(), __FILE__, __LINE__);
	}

	result = base::PubStr::Double2FormatStr(dou_res);
}

bool YCStatFactor::IsCategoryDim(const std::string& dim)
{
	const int VEC_TOP_SIZE = m_vTopStatInfo.size();
	for ( int i = 0; i < VEC_TOP_SIZE; ++i )
	{
		YCStatInfo& ref_si = m_vTopStatInfo[i];

		if ( ref_si.category && dim == ref_si.statdim_id )
		{
			return true;
		}
	}

	for ( MAP_VEC_STATINFO::iterator m_it = m_mvStatInfo.begin(); m_it != m_mvStatInfo.end(); ++m_it )
	{
		const int VEC_SIZE = m_it->second.size();
		for ( int j = 0; j < VEC_SIZE; ++j )
		{
			YCStatInfo& ref_si = (m_it->second)[j];

			if ( ref_si.category && dim == ref_si.statdim_id )
			{
				return true;
			}
		}
	}

	return false;
}

void YCStatFactor::MakeStatInfoResult(int batch, const std::string& city, const YCStatInfo& st_info, bool agg, std::vector<std::vector<std::string> >& vec2_result) throw(base::Exception)
{
	YCStatResult yc_sr;
	yc_sr.stat_city   = city;
	yc_sr.stat_batch  = batch;
	yc_sr.stat_report = st_info.stat_report;
	yc_sr.stat_id     = st_info.stat_id;
	yc_sr.stat_name   = st_info.stat_name;
	yc_sr.statdim_id  = st_info.statdim_id;

	std::vector<std::string> vec_data;
	if ( st_info.category )		// 分类因子
	{
		VEC_CATEGORYFACTOR vec_ctg_factor;
		ExpandCategoryStatInfo(st_info, agg, vec_ctg_factor);

		// 由分类因子信息生成结果数据
		const int VEC_SIZE = vec_ctg_factor.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			YCCategoryFactor& ref_cf = vec_ctg_factor[i];
			yc_sr.statdim_id = ref_cf.dim_id;
			yc_sr.stat_value = ref_cf.value;

			// 一般因子记录数据项名称
			if ( !agg )
			{
				yc_sr.stat_name = ref_cf.item;
			}

			// 记录分类因子结果
			if ( m_mFactor.find(yc_sr.statdim_id) != m_mFactor.end() )
			{
				throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "重复的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", ref_cf.dim_id.c_str(), __FILE__, __LINE__);
			}
			else
			{
				m_mFactor[yc_sr.statdim_id] = yc_sr.stat_value;
			}

			if ( agg )
			{
				m_pLog->Output("[YCStatFactor] 因子结果：DIM=[%s], VALUE=[%s]", yc_sr.statdim_id.c_str(), yc_sr.stat_value.c_str());
			}
			else
			{
				m_pLog->Output("[YCStatFactor] 因子结果：ITEM=[%s], DIM=[%s], VALUE=[%s]", yc_sr.stat_name.c_str(), yc_sr.statdim_id.c_str(), yc_sr.stat_value.c_str());
			}

			yc_sr.Convert2Vector(vec_data);
			base::PubStr::VVectorSwapPushBack(vec2_result, vec_data);
		}
	}
	else	// 非分类因子
	{
		if ( agg )	// 组合因子
		{
			yc_sr.stat_value = CalcComplexFactor(st_info.stat_sql);

			// 记录组合因子结果
			if ( m_mFactor.find(yc_sr.statdim_id) != m_mFactor.end() )
			{
				throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "重复的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", st_info.statdim_id.c_str(), __FILE__, __LINE__);
			}
			else
			{
				m_mFactor[yc_sr.statdim_id] = yc_sr.stat_value;
			}
		}
		else	// 一般因子
		{
			MAP_FACTOR::iterator m_it = m_mFactor.find(base::PubStr::TrimUpperB(st_info.statdim_id));
			if ( m_it != m_mFactor.end() )
			{
				yc_sr.stat_value = m_it->second;
			}
			else
			{
				throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "不存在的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", st_info.statdim_id.c_str(), __FILE__, __LINE__);
			}
		}

		m_pLog->Output("[YCStatFactor] 因子结果：DIM=[%s], VALUE=[%s]", yc_sr.statdim_id.c_str(), yc_sr.stat_value.c_str());
		yc_sr.Convert2Vector(vec_data);
		base::PubStr::VVectorSwapPushBack(vec2_result, vec_data);
	}
}

void YCStatFactor::ExpandCategoryStatInfo(const YCStatInfo& st_info, bool agg, VEC_CATEGORYFACTOR& vec_ctgfctr) throw(base::Exception)
{
	VEC_CATEGORYFACTOR       vec_cf;
	std::vector<std::string> vec_fmt;

	if ( agg )	// 组合因子
	{
		// 参考格式：因子"A?_B?"，其表达式"A?,B?|-"
		base::PubStr::Str2StrVector(st_info.stat_sql, "|", vec_fmt);
		if ( vec_fmt.size() != 2 )
		{
			throw base::Exception(ANAERR_EXPAND_CATEGORY_INFO, "无法识别的组合分类因子表达式：%s [FILE:%s, LINE:%d]", st_info.stat_sql.c_str(), __FILE__, __LINE__);
		}

		std::string dims = base::PubStr::TrimUpperB(vec_fmt[0]);
		std::string oper = base::PubStr::TrimUpperB(vec_fmt[1]);

		std::vector<std::string> vec_oper;
		base::PubStr::Str2StrVector(dims, ",", vec_fmt);
		base::PubStr::Str2StrVector(oper, ",", vec_oper);

		// 至少两个统计维度；且运算符个数比维度少一个
		const int VEC_DIM_SIZE = vec_fmt.size();
		if ( VEC_DIM_SIZE < 2 || (size_t)VEC_DIM_SIZE != (vec_oper.size() + 1) )
		{
			throw base::Exception(ANAERR_EXPAND_CATEGORY_INFO, "不匹配的组合分类因子表达式：%s [FILE:%s, LINE:%d]", st_info.stat_sql.c_str(), __FILE__, __LINE__);
		}

		int              index = 1;
		std::string      fctr_value;
		std::string      cmpl_fctr_result;
		YCCategoryFactor ctg_factor;

		// 计算结果
		while ( true )
		{
			bool break_out = false;
			cmpl_fctr_result.clear();			// 结果值初始化（清空）

			for ( int i = 1; i < VEC_DIM_SIZE; ++i )
			{
				if ( 1 == i )	// 首次
				{
					if ( GetCategoryFactorValue(vec_fmt[0], index, fctr_value) )
					{
						OperateOneFactor(cmpl_fctr_result, "+", fctr_value);
					}
					else
					{
						break_out = true;
						break;
					}
				}

				if ( GetCategoryFactorValue(vec_fmt[i], index, fctr_value) )
				{
					OperateOneFactor(cmpl_fctr_result, vec_oper[i-1], fctr_value);
				}
				else
				{
					break_out = true;
					break;
				}
			}

			if ( break_out )
			{
				break;
			}

			ctg_factor.dim_id = ExtendCategoryDim(st_info.statdim_id, index);
			ctg_factor.value  = cmpl_fctr_result;
			vec_cf.push_back(ctg_factor);

			++index;
		}
	}
	else	// 一般因子
	{
		// 参考格式：因子"A?(B?)"、"B?"、"A?(0)"
		const std::string DIM = base::PubStr::TrimUpperB(st_info.statdim_id);
		if ( DIM.find('(') != std::string::npos )	// 主因子
		{
			base::PubStr::Str2StrVector(DIM, "(", vec_fmt);
			if ( vec_fmt.size() != 2 )
			{
				throw base::Exception(ANAERR_EXPAND_CATEGORY_INFO, "无法识别的组合分类因子维度：%s [FILE:%s, LINE:%d]", DIM.c_str(), __FILE__, __LINE__);
			}

			std::string dim_B = vec_fmt[1];
			if ( dim_B.size() < 2 || dim_B[dim_B.size()-1] != ')' )
			{
				throw base::Exception(ANAERR_EXPAND_CATEGORY_INFO, "无法识别的组合分类因子维度：%s [FILE:%s, LINE:%d]", DIM.c_str(), __FILE__, __LINE__);
			}
			dim_B = base::PubStr::TrimB(dim_B.substr(0,dim_B.size()-1));

			std::string dim_A = vec_fmt[0];
			MatchCategoryFactor(DIM, dim_A, dim_B, vec_cf);
		}
		else	// 辅助因子
		{
			m_pLog->Output("[YCStatFactor] 一般辅助分类因子：%s [IGNORED]", DIM.c_str());
		}
	}

	vec_cf.swap(vec_ctgfctr);
}

void YCStatFactor::MatchCategoryFactor(const std::string& dim, const std::string& dim_a, const std::string& dim_b, VEC_CATEGORYFACTOR& vec_ctgfctr)
{
	VEC_CATEGORYFACTOR vec_cf_A;
	MAP_VEC_CATEGORYFACTOR::iterator m_it = m_mvCategoryFactor.find(dim);
	if ( m_it != m_mvCategoryFactor.end() )
	{
		vec_cf_A.assign(m_it->second.begin(), m_it->second.end());
	}

	YCCategoryFactor ctg_factor;
	if ( dim_b != "0" )		// 包含辅助因子，匹配关系 A<->B
	{
		VEC_CATEGORYFACTOR vec_cf_B;
		m_it = m_mvCategoryFactor.find(dim_b);
		if ( m_it != m_mvCategoryFactor.end() )
		{
			vec_cf_B.assign(m_it->second.begin(), m_it->second.end());
		}

		int index = 1;
		YCPairCategoryFactor pcf;
		MAP_PAIRCATEGORYFACTOR mItemPairCF;
		int vec_size = vec_cf_A.size();
		for ( int i = 0; i < vec_size; ++i )
		{
			ctg_factor = vec_cf_A[i];
			if ( mItemPairCF.find(ctg_factor.item) != mItemPairCF.end() )
			{
				throw base::Exception(ANAERR_MATCH_CATEGORY_FACTOR, "分类因子数据项重复：DIM=[%s], ITEM=[%s], VALUE=[%s] [FILE:%s, LINE:%d]", ctg_factor.dim_id.c_str(), ctg_factor.item.c_str(), ctg_factor.value.c_str(), __FILE__, __LINE__);
			}

			pcf.index         = index++;
			ctg_factor.dim_id = ExtendCategoryDim(dim_a, pcf.index);
			pcf.cf_A          = ctg_factor;
			pcf.cf_B.dim_id   = ExtendCategoryDim(dim_b, pcf.index);
			pcf.cf_B.item     = ctg_factor.item;
			pcf.cf_B.value    = "0";
			mItemPairCF[ctg_factor.item] = pcf;
		}

		std::set<std::string> sItemRep;
		MAP_PAIRCATEGORYFACTOR::iterator it;
		vec_size = vec_cf_B.size();
		for ( int j = 0; j < vec_size; ++j )
		{
			ctg_factor = vec_cf_B[j];
			if ( sItemRep.find(ctg_factor.item) != sItemRep.end() )
			{
				throw base::Exception(ANAERR_MATCH_CATEGORY_FACTOR, "分类因子数据项重复：DIM=[%s], ITEM=[%s], VAL=[%s] [FILE:%s, LINE:%d]", ctg_factor.dim_id.c_str(), ctg_factor.item.c_str(), ctg_factor.value.c_str(), __FILE__, __LINE__);
			}
			sItemRep.insert(ctg_factor.item);

			if ( (it = mItemPairCF.find(ctg_factor.item)) != mItemPairCF.end() )	// 数据项已存在
			{
				ctg_factor.dim_id = ExtendCategoryDim(dim_b, it->second.index);
				it->second.cf_B   = ctg_factor;
			}
			else	// 数据项不存在
			{
				pcf.index         = index++;
				ctg_factor.dim_id = ExtendCategoryDim(dim_b, pcf.index);
				pcf.cf_A.dim_id   = ExtendCategoryDim(dim_a, pcf.index);
				pcf.cf_A.item     = ctg_factor.item;
				pcf.cf_A.value    = "0";
				pcf.cf_B          = ctg_factor;
				mItemPairCF[ctg_factor.item] = pcf;
			}
		}

		if ( mItemPairCF.empty() )
		{
			throw base::Exception(ANAERR_MATCH_CATEGORY_FACTOR, "没有分类因子维度对应的源数据：[%s] and [%s] [FILE:%s, LINE:%d]", dim.c_str(), dim_b.c_str(), __FILE__, __LINE__);
		}

		for ( it = mItemPairCF.begin(); it != mItemPairCF.end(); ++it )
		{
			vec_ctgfctr.push_back(it->second.cf_A);
			vec_ctgfctr.push_back(it->second.cf_B);
		}
	}
	else	// 不含辅助因子
	{
		if ( vec_cf_A.empty() )
		{
			throw base::Exception(ANAERR_MATCH_CATEGORY_FACTOR, "没有分类因子维度对应的源数据：[%s] [FILE:%s, LINE:%d]", dim.c_str(), __FILE__, __LINE__);
		}

		const int VEC_SIZE = vec_cf_A.size();
		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			ctg_factor        = vec_cf_A[i];
			ctg_factor.dim_id = ExtendCategoryDim(dim_a, (i+1));
			vec_ctgfctr.push_back(ctg_factor);
		}
	}
}

