#include "ycstatfactor.h"
#include "anaerror.h"

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

