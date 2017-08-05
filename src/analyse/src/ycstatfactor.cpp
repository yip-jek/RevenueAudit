#include "ycstatfactor.h"
#include "anaerror.h"
#include "log.h"

const char* const YCStatFactor::S_TOP_PRIORITY  = "NN";			// 最高优先级 (差异汇总)

YCStatFactor::YCStatFactor(YCTaskReq& task_req)
:m_pLog(base::Log::Instance())
,m_pTaskReq(&task_req)
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

		base::PubStr::TrimUpper(ref_si.statdim_id);
		base::PubStr::TrimUpper(ref_si.stat_priority);

		// 是否为分类因子：维度 ID 包含问号
		ref_si.category = (ref_si.statdim_id.find('?') != std::string::npos);

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

int YCStatFactor::LoadFactors(const VEC3_STRING& v3_data) throw(base::Exception)
{
	ClearOldFactors();

	int         counter = 0;
	std::string dim_id;

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
				dim_id = base::PubStr::TrimUpperB(ref_vec[0]);

				LoadOneFactor(dim_id, VEC_STRING(ref_vec.begin()+1, ref_vec.end()));
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

