#include "ycresult_xqb.h"
#include "anaerror.h"
#include "pubstr.h"
#include "anataskinfo.h"

YCResult_XQB::YCResult_XQB(int ana_type, int item_size, int val_size)
:batch(0)
,ANA_TYPE(ana_type)
,m_factor(item_size, val_size)
{
}

YCResult_XQB::~YCResult_XQB()
{
}

void YCResult_XQB::ImportFactor(const YCFactor_XQB& factor)
{
	m_factor = factor;
}

bool YCResult_XQB::Import(const VEC_STRING& vec_dat)
{
	const bool   IS_ANATYPE_GD = (AnalyseRule::ANATYPE_YCXQB_GD == ANA_TYPE);
	const size_t MEMBER_SIZE   = S_PUBLIC_MEMBERS + m_factor.GetAllSize() + IS_ANATYPE_GD;
	if ( vec_dat.size() != MEMBER_SIZE )
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
	if ( IS_ANATYPE_GD && !base::PubStr::Str2Int(vec_dat[index++], batch) )
	{
		return false;
	}

	return m_factor.Import(VEC_STRING(vec_dat.begin()+index, vec_dat.end()));
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
	if ( AnalyseRule::ANATYPE_YCXQB_GD == ANA_TYPE )
	{
		v_dat.push_back(base::PubStr::Int2Str(batch));
	}
	v_dat.swap(vec_dat);

	m_factor.Export(v_dat);
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
										m_factor.LogPrintInfo().c_str());
	return info;
}

std::string YCResult_XQB::GetFactorDim() const
{
	return m_factor.GetDimID();
}

std::string YCResult_XQB::GetFactorArea() const
{
	return m_factor.GetArea();
}

std::string YCResult_XQB::GetFactorFirstItem() const
{
	VEC_STRING vec_item;
	m_factor.ExportItems(vec_item);

	return vec_item[0];
}

std::string YCResult_XQB::GetFactorFirstValue() const
{
	VEC_STRING vec_val;
	m_factor.ExportValue(vec_val);

	return vec_val[0];
}

