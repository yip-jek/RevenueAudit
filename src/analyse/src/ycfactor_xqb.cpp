#include "ycfactor_xqb.h"
#include "pubstr.h"

YCFactor_XQB_YCW::YCFactor_XQB_YCW()
{
}

YCFactor_XQB_YCW::~YCFactor_XQB_YCW()
{
}

bool YCFactor_XQB_YCW::Import(const VEC_STRING& vec_dat)
{
	const int VEC_SIZE = vec_dat.size();
	if ( VEC_SIZE != S_XQBYCW_MEMBERS )
	{
		return false;
	}

	int index = 0;
	dim_id = vec_dat[index++];
	area   = vec_dat[index++];
	item   = vec_dat[index++];
	value  = vec_dat[index++];
	return true;
}

void YCFactor_XQB_YCW::Export(VEC_STRING& vec_dat) const
{
	VEC_STRING v_dat;
	v_dat.push_back(dim_id);
	v_dat.push_back(area);
	v_dat.push_back(item);
	v_dat.push_back(value);

	v_dat.swap(vec_dat);
}

std::string YCFactor_XQB_YCW::LogPrintInfo() const
{
	std::string info;
	base::PubStr::SetFormatString(info, "DIM=[%s], "
										"AREA=[%s], "
										"ITEM=[%s], "
										"VALUE=[%s]"
										dim_id.c_str(), 
										area.c_str(), 
										item.c_str(), 
										value.c_str());
	return info;
}


////////////////////////////////////////////////////////////////////////////////
YCFactor_XQB_GD::YCFactor_XQB_GD()
{
}

YCFactor_XQB_GD::~YCFactor_XQB_GD()
{
}

virtual bool YCFactor_XQB_GD::Import(const VEC_STRING& vec_dat)
{
}

virtual void YCFactor_XQB_GD::Export(VEC_STRING& vec_dat) const
{
}

std::string YCFactor_XQB_GD::LogPrintInfo() const
{
}

