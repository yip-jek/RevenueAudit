#include "ycstatfactor.h"
#include "anaerror.h"
#include "pubstr.h"
#include "log.h"

const char* const YCStatFactor::S_TOP_PRIORITY  = "NN";			// 最高优先级


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

void YCStatFactor::LoadStatInfo(std::vector<YCStatInfo>& vec_statinfo) throw(base::Exception)
{
	m_statID.clear();
	m_statReport.clear();

	if ( !m_mvStatInfo.empty() )
	{
		m_mvStatInfo.clear();
	}
	if ( !m_vTopStatInfo.empty() )
	{
		std::vector<YCStatInfo>().swap(m_vTopStatInfo);
	}

	const int VEC_SIZE = vec_statinfo.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YCStatInfo& ref_si = vec_statinfo[i];

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

int YCStatFactor::LoadDimFactor(std::vector<std::vector<std::vector<std::string> > >& v3_data) throw(base::Exception)
{
	if ( !m_mDimFactor.empty() )
	{
		m_mDimFactor.clear();
	}

	int              counter = 0;
	std::string      dim;
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
			if ( VEC_SIZE == S_BASE_COLUMN_SIZE )
			{
				dim = base::PubStr::TrimUpperB(ref_vec[0]);
				if ( m_mDimFactor.find(dim) != m_mDimFactor.end() )
				{
					throw base::Exception(ANAERR_LOAD_DIM_FACTOR, "重复的维度因子：[%s] [FILE:%s, LINE:%d]", dim.c_str(), __FILE__, __LINE__);
				}

				m_mDimFactor[dim] = base::PubStr::TrimB(ref_vec[1]);
			}
			else if ( VEC_SIZE == S_CATEGORY_COLUMN_SIZE )
			{
				dim = base::PubStr::TrimUpperB(ref_vec[0]);
				if ( !IsCategoryDim(dim) )
				{
					throw base::Exception(ANAERR_LOAD_DIM_FACTOR, "业财采集结果数据错误，数据 SIZE 不匹配：[%lu] [FILE:%s, LINE:%d]", VEC_SIZE, __FILE__, __LINE__);
				}

				ctgFactor.dim_id = dim;
				ctgFactor.item   = base::PubStr::TrimB(ref_vec[1]);
				ctgFactor.value  = base::PubStr::TrimB(ref_vec[2]);
				m_mvCategoryFactor[dim].push_back(ctgFactor);
			}
			else
			{
				throw base::Exception(ANAERR_LOAD_DIM_FACTOR, "业财采集结果数据错误，数据 SIZE 不匹配：[%lu] [FILE:%s, LINE:%d]", VEC_SIZE, __FILE__, __LINE__);
			}

			++counter;
		}
	}

	m_pLog->Output("[YCStatFactor] 载入维度因子对成功.");
	return counter;
}

void YCStatFactor::GenerateResult(int batch, const std::string& city, std::vector<std::vector<std::string> >& v2_result) throw(base::Exception)
{
	std::vector<std::vector<std::string> > vec2_result;

	// 生成一般因子与组合因子的结果数据
	for ( std::map<int, std::vector<YCStatInfo> >::iterator m_it = m_mvStatInfo.begin(); m_it != m_mvStatInfo.end(); ++m_it )
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

	// 生成汇总因子的结果数据
	const int VEC_TOP_SIZE = m_vTopStatInfo.size();
	for ( int n = 0; n < VEC_TOP_SIZE; ++n )
	{
		YCStatInfo& ref_si = m_vTopStatInfo[n];

		m_pLog->Output("[YCStatFactor] 统计因子类型：汇总因子");
		m_pLog->Output("[YCStatFactor] 生成汇总因子结果数据：STAT_ID:%s, STATDIM_ID:%s, STAT_PRIORITY:%s", ref_si.stat_id.c_str(), ref_si.statdim_id.c_str(), ref_si.stat_priority.c_str());
		MakeStatInfoResult(batch, city, ref_si, true, vec2_result);
	}

	vec2_result.swap(v2_result);
}

std::string YCStatFactor::CalcComplexFactor(const std::string& cmplx_fctr_fmt) throw(base::Exception)
{
	m_pLog->Output("[YCStatFactor] 组合因子表达式：%s", cmplx_fctr_fmt.c_str());

	// 组合因子格式：{A?_B?}
	if ( cmplx_fctr_fmt.find('?') != std::string::npos )
	{
		return CalcCategoryFactor(cmplx_fctr_fmt);
	}

	// 组合因子格式：[ A1, A2, A3, ...|+, -, ... ]
	std::vector<std::string> vec_fmt_left;
	base::PubStr::Str2StrVector(cmplx_fctr_fmt, "|", vec_fmt_left);
	if ( vec_fmt_left.size() != 2 )
	{
		throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "无法识别的组合因子表达式：%s [FILE:%s, LINE:%d]", cmplx_fctr_fmt.c_str(), __FILE__, __LINE__);
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

	// 计算结果
	std::string cmplx_fctr_result;
	std::map<std::string, std::string>::iterator m_it;
	for ( int i = 1; i < VEC_DIM_SIZE; ++i )
	{
		if ( 1 == i )	// 首次
		{
			if ( (m_it = m_mDimFactor.find(vec_fmt_left[0])) != m_mDimFactor.end() )
			{
				OperateOneFactor(cmplx_fctr_result, "+", m_it->second);
			}
			else
			{
				throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", vec_fmt_left[0].c_str(), __FILE__, __LINE__);
			}
		}

		if ( (m_it = m_mDimFactor.find(vec_fmt_left[i])) != m_mDimFactor.end() )
		{
			OperateOneFactor(cmplx_fctr_result, vec_fmt_right[i-1], m_it->second);
		}
		else
		{
			throw base::Exception(ANAERR_CALC_COMPLEX_FACTOR, "不存在的业财稽核统计维度ID：%s [FILE:%s, LINE:%d]", vec_fmt_left[i].c_str(), __FILE__, __LINE__);
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

		m_pLog->Output("[YCStatFactor] 组合分类因子表达式 [%s] 共包含 [%d] 个分类因子", ctg_fmt.c_str(), (ctg_index-1));
		return ctg_result;
	}
	else
	{
		throw base::Exception(ANAERR_CALC_CATEGORY_FACTOR, "无法识别的组合分类因子表达式：%s [FILE:%s, LINE:%d]", ctg_fmt.c_str(), __FILE__, __LINE__);
	}
}

void YCStatFactor::ExtendCategoryDim(std::string& dim, int index)
{
	const std::string STR_INDEX = base::PubStr::Int2Str(index);

	// 将问号（?）替换为 index
	size_t pos = 0;
	while ( (pos = dim.find('?')) != std::string::npos )
	{
		dim.replace(pos, 1, STR_INDEX);
	}
}

bool YCStatFactor::GetCategoryFactorValue(const std::string& ctg_fmt, int index, std::string& val)
{
	std::string dim_id = ctg_fmt;
	ExtendCategoryDim(dim_id, index);

	std::map<std::string, std::string>::iterator m_it = m_mDimFactor.find(dim_id);
	if ( m_it != m_mDimFactor.end() )
	{
		val = m_it->second;
		return true;
	}

	return false;
}

void YCStatFactor::OperateOneFactor(std::string& result, const std::string& op, const std::string& factor) throw(base::Exception)
{
	double d_res = 0.0;
	if ( !result.empty() && !base::PubStr::Str2Double(result, d_res) )
	{
		throw base::Exception(ANAERR_OPERATE_ONE_FACTOR, "结果值无法转化为精度型：%s [FILE:%s, LINE:%d]", result.c_str(), __FILE__, __LINE__);
	}

	double d_fctr = 0.0;
	if ( factor.empty() || !base::PubStr::Str2Double(factor, d_fctr) )
	{
		throw base::Exception(ANAERR_OPERATE_ONE_FACTOR, "非有效因子值：%s [FILE:%s, LINE:%d]", factor.c_str(), __FILE__, __LINE__);
	}

	if ( "+" == op )
	{
		d_res += d_fctr;
	}
	else if ( "-" == op )
	{
		d_res -= d_fctr;
	}
	else if ( "*" == op )
	{
		d_res *= d_fctr;
	}
	else if ( "/" == op )
	{
		d_res /= d_fctr;
	}
	else
	{
		throw base::Exception(ANAERR_OPERATE_ONE_FACTOR, "无法识别的因子运算符：%s [FILE:%s, LINE:%d]", op.c_str(), __FILE__, __LINE__);
	}

	result = base::PubStr::Double2Str(d_res);
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

	for ( std::map<int, std::vector<YCStatInfo> >::iterator m_it = m_mvStatInfo.begin(); m_it != m_mvStatInfo.end(); ++m_it )
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
		std::vector<YCCategoryFactor> vec_ctg_factor;
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
			if ( m_mDimFactor.find(yc_sr.statdim_id) != m_mDimFactor.end() )
			{
				throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "重复的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", ref_cf.dim_id.c_str(), __FILE__, __LINE__);
			}
			else
			{
				m_mDimFactor[yc_sr.statdim_id] = yc_sr.stat_value;
			}

			yc_sr.Trans2Vector(vec_data);
			base::PubStr::VVectorSwapPushBack(vec2_result, vec_data);
		}
	}
	else	// 非分类因子
	{
		if ( agg )	// 组合因子
		{
			yc_sr.stat_value = CalcComplexFactor(st_info.stat_sql);

			// 记录组合因子结果
			if ( m_mDimFactor.find(yc_sr.statdim_id) != m_mDimFactor.end() )
			{
				throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "重复的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", st_info.statdim_id.c_str(), __FILE__, __LINE__);
			}
			else
			{
				m_mDimFactor[yc_sr.statdim_id] = yc_sr.stat_value;
			}
		}
		else	// 一般因子
		{
			std::map<std::string, std::string>::iterator m_it = m_mDimFactor.find(st_info.statdim_id);
			if ( m_it != m_mDimFactor.end() )
			{
				yc_sr.stat_value = m_it->second;
			}
			else
			{
				throw base::Exception(ANAERR_MAKE_STATINFO_RESULT, "不存在的业财稽核统计维度ID: %s [FILE:%s, LINE:%d]", st_info.statdim_id.c_str(), __FILE__, __LINE__);
			}
		}

		yc_sr.Trans2Vector(vec_data);
		base::PubStr::VVectorSwapPushBack(vec2_result, vec_data);
	}
}

void YCStatFactor::ExpandCategoryStatInfo(const YCStatInfo& st_info, bool agg, std::vector<YCCategoryFactor>& vec_ctgfctr) throw(base::Exception)
{
	std::vector<YCCategoryFactor> vec_cf;
	if ( agg )	// 组合因子
	{
	}
	else	// 一般因子
	{
	}

	vec_cf.swap(vec_ctgfctr);
}

