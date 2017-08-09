#include "ycresult_xqb.h"
#include "ycfactor_xqb.h"
#include "anaerror.h"
#include "pubstr.h"

YCResult_XQB::YCResult_XQB(RESULT_FACTOR_TYPE rf_type, int item_size)
:batch(0)
,m_rfType(rf_type)
,ITEM_SIZE(item_size)
,m_pFactor(NULL)
{
	CreateFactor();
}

YCResult_XQB::YCResult_XQB(const YCResult_XQB& ycr)
:bill_cyc(ycr.bill_cyc)
,city(ycr.city)
,type(ycr.type)
,batch(ycr.batch)
,m_rfType(ycr.m_rfType)
,ITEM_SIZE(ycr.ITEM_SIZE)
,m_pFactor(NULL)
{
	CreateFactor();
	ImportFromFactor(ycr.m_pFactor);
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
		m_pFactor = new YCFactor_XQB_YCW(ITEM_SIZE);
	}
	else if ( RFT_XQB_GD == m_rfType )			// 详情表（省）因子
	{
		m_pFactor = new YCFactor_XQB_GD(ITEM_SIZE);
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
	const int VEC_SIZE    = vec_dat.size();
	const int MEMBER_SIZE = S_PUBLIC_MEMBERS + (RFT_XQB_GD == m_rfType) + m_pFactor->GetAllSize();
	if ( VEC_SIZE != MEMBER_SIZE )
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

	// 详情表（省）稽核有2个批次：业务批次 和 财务批次
	// 而且批次是一致的
	if ( RFT_XQB_GD == m_rfType && !base::PubStr::Str2Int(vec_dat[index++], batch) )
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

	// 详情表（省）稽核有2个批次：业务批次 和 财务批次
	// 而且批次是一致的
	if ( RFT_XQB_GD == m_rfType )
	{
		v_dat.push_back(base::PubStr::Int2Str(batch));
	}

	v_dat.swap(vec_dat);

	m_pFactor->Export(v_dat);
	vec_dat.insert(vec_dat.end(), v_dat.begin(), v_dat.end());
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

std::string YCResult_XQB::GetFactorDim() const
{
	return m_pFactor->GetDimID();
}

std::string YCResult_XQB::GetFactorArea() const
{
	return m_pFactor->GetArea();
}

std::string YCResult_XQB::GetFactorFirstItem() const
{
	VEC_STRING vec_item;
	m_pFactor->ExportItems(vec_item);

	return vec_item[0];
}

std::string YCResult_XQB::GetFactorFirstValue() const
{
	VEC_STRING vec_val;
	m_pFactor->ExportValue(vec_val);

	return vec_val[0];
}

