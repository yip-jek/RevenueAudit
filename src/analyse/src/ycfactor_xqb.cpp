#include "ycfactor_xqb.h"
#include "anaerror.h"
#include "pubstr.h"

YCFactor_XQB::YCFactor_XQB(int item_size)
:ITEM_SIZE(item_size)
{
	InitItems();
}

YCFactor_XQB::~YCFactor_XQB()
{
}

bool YCFactor_XQB::Import(const VEC_STRING& vec_dat)
{
	const int VEC_SIZE = vec_dat.size();
	if ( VEC_SIZE != GetAllSize() )
	{
		return false;
	}

	int index = 0;
	m_dimID = vec_dat[index++];
	m_area  = vec_dat[index++];

	const int LAST_INDEX = index + ITEM_SIZE;
	if ( !ImportItems(VEC_STRING(vec_dat.begin()+index, vec_dat.begin()+LAST_INDEX)) )
	{
		return false;
	}

	return ImportValue(VEC_STRING(vec_dat.begin()+LAST_INDEX, vec_dat.end()));
}

void YCFactor_XQB::Export(VEC_STRING& vec_dat) const
{
	VEC_STRING v_dat;
	v_dat.push_back(m_dimID);
	v_dat.push_back(m_area);
	v_dat.insert(v_dat.end(), m_vecItems.begin(), m_vecItems.end());
	v_dat.swap(vec_dat);

	ExportValue(v_dat);
	vec_dat.insert(vec_dat.end(), v_dat.begin(), v_dat.end());
}

std::string YCFactor_XQB::LogPrintInfo() const
{
	std::string info;
	base::PubStr::SetFormatString(info, "DIM=[%s], "
										"AREA=[%s], ", 
										m_dimID.c_str(), 
										m_area.c_str());

	for ( int i = 0; i < ITEM_SIZE; ++i )
	{
		base::PubStr::SetFormatString(info, "%sITEM_%d=[%s], ", info.c_str(), (i+1), m_vecItems[i].c_str());
	}

	AddValueLogInfo(info);
	return info;
}

int YCFactor_XQB::GetAllSize() const
{
	// 1个DIM_ID + 1个AREA + ITEM_SIZE
	return (2 + ITEM_SIZE);
}

int YCFactor_XQB::GetAreaItemSize() const
{
	// 1个AREA + ITEM_SIZE
	return (1 + ITEM_SIZE);
}

std::string YCFactor_XQB::GetDimID() const
{
	return m_dimID;
}

std::string YCFactor_XQB::GetArea() const
{
	return m_area;
}

void YCFactor_XQB::SetDimID(const std::string& dim)
{
	m_dimID = dim;
}

void YCFactor_XQB::SetArea(const std::string& area)
{
	m_area = area;
}

bool YCFactor_XQB::ImportItems(const VEC_STRING& vec_item)
{
	const int VEC_SIZE = vec_item.size();
	for ( VEC_SIZE != ITEM_SIZE )
	{
		return false;
	}

	m_vecItems.assign(vec_item.begin(), vec_item.end());
	return true;
}

void YCFactor_XQB::ExportItems(VEC_STRING& vec_item)
{
	vec_item.assign(m_vecItems.begin(), m_vecItems.end());
}

void YCFactor_XQB::InitItems() throw(base::Exception)
{
	if ( ITEM_SIZE <= 0 )
	{
		throw base::Exception(ANAERR_INIT_ITEMS, "Init items failed! Invalid item size: [%d] [FILE:%s, LINE:%d]", ITEM_SIZE, __FILE__, __LINE__);
	}

	// 初始化：空字符串
	m_vecItems.assign(ITEM_SIZE, "");
}


////////////////////////////////////////////////////////////////////////////////
YCFactor_XQB_YCW::YCFactor_XQB_YCW(int item_size)
:YCFactor_XQB(item_size)
{
}

YCFactor_XQB_YCW::~YCFactor_XQB_YCW()
{
}

int YCFactor_XQB_YCW::GetAllSize() const
{
	return (YCFactor_XQB::GetAllSize() + S_VALUE_SIZE);
}

int YCFactor_XQB_YCW::GetValueSize() const
{
	return S_VALUE_SIZE;
}

bool YCFactor_XQB_YCW::ImportValue(const VEC_STRING& vec_value)
{
	if ( vec_value.size() == S_VALUE_SIZE )
	{
		m_value = vec_value[0];
		return true;
	}

	return false;
}

void YCFactor_XQB_YCW::ExportValue(VEC_STRING& vec_value) const
{
	vec_value.assign(1, m_value);
}

void YCFactor_XQB_YCW::AddValueLogInfo(std::string& info) const
{
	base::PubStr::SetFormatString(info, "%sVALUE=[%s]", info.c_str(), m_value.c_str());
}


////////////////////////////////////////////////////////////////////////////////
YCFactor_XQB_GD::YCFactor_XQB_GD(int item_size)
:YCFactor_XQB(item_size)
{
}

YCFactor_XQB_GD::~YCFactor_XQB_GD()
{
}

int YCFactor_XQB_GD::GetAllSize() const
{
	return (YCFactor_XQB::GetAllSize() + S_VALUE_SIZE);
}

int YCFactor_XQB_GD::GetValueSize() const
{
	return S_VALUE_SIZE;
}

bool YCFactor_XQB_GD::ImportValue(const VEC_STRING& vec_value)
{
	if ( vec_value.size() == S_VALUE_SIZE )
	{
		m_valueYW = vec_value[0];
		m_valueCW = vec_value[1];
		return true;
	}

	return false;
}

void YCFactor_XQB_GD::ExportValue(VEC_STRING& vec_value) const
{
	VEC_STRING v_value;
	v_value.push_back(m_valueYW);
	v_value.push_back(m_valueCW);
	v_value.swap(vec_value);
}

void YCFactor_XQB_GD::AddValueLogInfo(std::string& info) const
{
	base::PubStr::SetFormatString(info, "%s"
										"VALUE_YW=[%s], "
										"VALUE_CW=[%s]", 
										info.c_str(), 
										m_valueYW.c_str(), 
										m_valueCW.c_str());
}

