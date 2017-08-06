#include "ycstatfactor_xqb.h"
#include "anaerror.h"
#include "log.h"

YCStatFactor_XQB::YCStatFactor_XQB(YCTaskReq& task_req)
:YCStatFactor(task_req)
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

	if ( !m_mFactor.empty() )
	{
		MAP_FACTOR::iterator it = m_mFactor.begin();
		const int   FACTOR_SIZE = it->second.Size();

		const int VEC_DAT_SIZE = vec_dat.size();
		if ( VEC_DAT_SIZE != FACTOR_SIZE )
		{
			throw base::Exception(ANAERR_LOAD_FACTOR, "业财采集结果数据size不一致：(原) DIM=[%s], SIZE=[%d] (新) DIM=[%s], SIZE=[%d] [FILE:%s, LINE:%d]", it->first.c_str(), FACTOR_SIZE, dim.c_str(), VEC_DAT_SIZE, __FILE__, __LINE__);
		}
	}

	m_mFactor[dim] = vec_dat;
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
	if ( agg )	// 组合因子
	{
	}
	else	// 一般因子
	{
	}
}

