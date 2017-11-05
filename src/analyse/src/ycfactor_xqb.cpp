#include "ycfactor_xqb.h"
#include "anaerror.h"
#include "pubstr.h"

YCFactor_XQB::YCFactor_XQB(int item_size, int val_size)
{
	Init(item_size, val_size);
}

YCFactor_XQB::~YCFactor_XQB()
{
}

bool YCFactor_XQB::Import(const VEC_STRING& vec_dat)
{
	if ( vec_dat.size() != (size_t)GetAllSize() )
	{
		return false;
	}

	int index = 0;
	m_dimID = vec_dat[index++];
	m_area  = vec_dat[index++];

	const int LAST_INDEX = index + m_vecItems.size();
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

	int vec_size = m_vecItems.size();
	for ( int i = 0; i < vec_size; ++i )
	{
		base::PubStr::SetFormatString(info, "%sITEM_%d=[%s], ", info.c_str(), (i+1), m_vecItems[i].c_str());
	}

	vec_size = m_vecVals.size();
	for ( int j = 0; j < vec_size; ++j )
	{
		base::PubStr::SetFormatString(info, "%sVALUE_%d=[%s], ", info.c_str(), (j+1), m_vecVals[j].c_str());
	}

	// 删除尾部的逗号与空格
	info.erase(info.size()-2);
	return info;
}

int YCFactor_XQB::GetAllSize() const
{
	// 1个DIM_ID + 1个AREA + m_vecItems.size() + m_vecVals.size()
	return (2 + m_vecItems.size() + m_vecVals.size());
}

int YCFactor_XQB::GetAreaItemSize() const
{
	// 1个AREA + m_vecItems.size()
	return (1 + m_vecItems.size());
}

int YCFactor_XQB::GetValueSize() const
{
	return m_vecVals.size();
}

std::string YCFactor_XQB::GetDimID() const
{
	return m_dimID;
}

std::string YCFactor_XQB::GetArea() const
{
	return m_area;
}

std::string YCFactor_XQB::GetItem(size_t index) const
{
	return ((index >= 0 && index < m_vecItems.size()) ? m_vecItems[index] : "");
}

std::string YCFactor_XQB::GetValue(size_t index) const
{
	return ((index >= 0 && index < m_vecVals.size()) ? m_vecVals[index] : "");
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
	if ( vec_item.size() == m_vecItems.size() )
	{
		m_vecItems.assign(vec_item.begin(), vec_item.end());
		return true;
	}

	return false;
}

void YCFactor_XQB::ExportItems(VEC_STRING& vec_item) const
{
	vec_item.assign(m_vecItems.begin(), m_vecItems.end());
}

bool YCFactor_XQB::ImportValue(const VEC_STRING& vec_value)
{
	const size_t VEC_VAL_SIZE = vec_value.size();
	if ( VEC_VAL_SIZE == m_vecVals.size() )
	{
		// 避免VALUE出现科学计数法（带E）
		for ( size_t i = 0; i < VEC_VAL_SIZE; ++i )
		{
			m_vecVals[i] = base::PubStr::StringDoubleFormat(vec_value[i]);
		}
		return true;
	}

	return false;
}

void YCFactor_XQB::ExportValue(VEC_STRING& vec_value) const
{
	vec_value.assign(m_vecVals.begin(), m_vecVals.end());
}

void YCFactor_XQB::Init(int item_size, int val_size) throw(base::Exception)
{
	if ( item_size <= 0 )
	{
		throw base::Exception(ANAERR_FACTOR_XQB_INIT, "Init items failed! Invalid item size: [%d] [FILE:%s, LINE:%d]", item_size, __FILE__, __LINE__);
	}

	// 初始化：空字符串
	m_vecItems.assign(item_size, "");

	if ( val_size <= 0 )
	{
		throw base::Exception(ANAERR_FACTOR_XQB_INIT, "Init values failed! Invalid value size: [%d] [FILE:%s, LINE:%d]", val_size, __FILE__, __LINE__);
	}

	// 初始化：空字符串
	m_vecVals.assign(val_size, "");
}

