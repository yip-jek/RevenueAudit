#include "ycresult_xqb.h"
#include "ycfactor_xqb.h"
#include "anaerror.h"
#include "pubstr.h"

YCResult_XQB::YCResult_XQB(RESULT_FACTOR_TYPE rf_type)
:batch(0)
,m_rfType(rf_type)
,m_pFactor(NULL)
{
	CreateFactor();
}

YCResult_XQB::~YCResult_XQB()
{
	ReleaseFactor();
}

void YCResult_XQB::CreateFactor() throw(base::Exception)
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
		throw base::Exception(ANAERR_CREATE_FACTOR, "Create factor failed! Unknown RESULT_FACTOR_TYPE: [%d] [FILE:%s, LINE:%d]", m_rfType, __FILE__, __LINE__);
	}

	if ( NULL == m_pFactor )
	{
		throw base::Exception(ANAERR_CREATE_FACTOR, "Create factor failed: Operator new YCFactor_XQB failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
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

bool YCResult_XQB::ImportFromFactor(const YCFactor_XQB* p_factor)
{
	if ( p_factor != NULL )
	{
		VEC_STRING vec_dat;
		p_factor->Export(vec_dat);
		return m_pFactor->Import(vec_dat);
	}

	return false;
}

bool YCResult_XQB::Import(const VEC_STRING& vec_dat)
{
	const int VEC_SIZE = vec_dat.size();
	if ( VEC_SIZE <= S_PUBLIC_MEMBERS )
	{
		return false;
	}

	int index = 0;
	bill_cyc = vec_dat[index++];
	city     = vec_dat[index++];
	type     = vec_dat[index++];

	if ( !base::PubStr::Str2Int(vec_dat[index++], batch) )
	{
		return false;
	}

	return m_pFactor->Import(VEC_STRING(vec_dat.begin()+index, vec_dat.end()));
}

void YCResult_XQB::Export(VEC_STRING& vec_dat) const
{
	VEC_STRING v_dat;
	v_dat.push_back(bill_cyc);
	v_dat.push_back(city);
	v_dat.push_back(type);
	v_dat.push_back(base::PubStr::Int2Str(batch));

	VEC_STRING v_factor;
	m_pFactor->Export(v_factor);
	v_dat.insert(v_dat.end(), v_factor.begin(), v_factor.end());

	v_dat.swap(vec_dat);
}

std::string YCResult_XQB::LogPrintInfo() const
{
	std::string info;
	base::PubStr::SetFormatString(info, "BILL_CYC=[%s], "
										"CITY=[%s], "
										"TYPE=[%s], "
										"BATCH=[%d], "
										"%s", 
										bill_cyc.c_str(), 
										city.c_str(), 
										type.c_str(), 
										batch, 
										m_pFactor->LogPrintInfo().c_str());
	return info;
}

