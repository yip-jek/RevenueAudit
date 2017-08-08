#include "ycresult_xqb.h"
#include "ycfactor_xqb.h"

YCResult_XQB::YCResult_XQB(RESULT_FACTOR_TYPE type)
:batch(0)
,m_rfType(type)
,m_pFactor(NULL)
{
	CreateFactor();
}

YCResult_XQB::~YCResult_XQB()
{
	ReleaseFactor();
}

void YCResult_XQB::CreateFactor()
{
	ReleaseFactor();

	if ( RFT_XQB_YCW == m_rfType )				// 详情表（业务侧、财务侧）因子
	{
		m_pFactor = new YCFactor_XQB_YCW();
	}
	else if ( RFT_XQB_GD == m_rfType )			// 详情表（省）因子
	{
		m_pFactor = new YCFactor_XQB_GD();
	}
	else	// 未知类型
	{
		m_pFactor = NULL;
	}
}

void YCResult_XQB::ReleaseFactor()
{
	if ( m_pFactor != NULL )
	{
		delete m_pFactor;
		m_pFactor = NULL;
	}
}

bool YCResult_XQB::ImportFromFactor(YCFactor_XQB* p_factor)
{
	if ( m_pFactor != NULL && p_factor != NULL )
	{
		std::vector<std::string> vec_dat;
		p_factor->Export(vec_dat);
		return m_pFactor->Import(vec_dat);
	}

	return false;
}

bool YCResult_XQB::Import(const std::vector<std::string>& vec_dat)
{
	if ( NULL == m_pFactor )
	{
		return false;
	}

	const int VEC_SIZE = vec_dat.size();
	if ( VEC_SIZE <
}

