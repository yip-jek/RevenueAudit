#include "config.h"
#include <sys/stat.h>
#include "def.h"
#include "pubstr.h"
#include "basefile.h"

namespace base
{

// class CfgItem
CfgItem::CfgItem()
:m_bFind(false)
{
}

CfgItem::CfgItem(const std::string& segment, const std::string& name, const std::string& val /*= std::string()*/, bool find /*= false*/)
:m_segment(segment)
,m_name(name)
,m_value(val)
,m_bFind(find)
{
}

////////////////////////////////////////////////////////////////////////

// class Config
Config::Config(const std::string& cfg_file /*= std::string()*/)
{
	//SetCfgFile(cfg_file);
	if ( !cfg_file.empty() && IsRegularFile(cfg_file) )
	{
		m_cfgFile = cfg_file;
	}
}

void Config::SetCfgFile(const std::string& cfg_file) throw(Exception)
{
	if ( cfg_file.empty() )
	{
		throw Exception(CFG_FILE_INVALID, "The configuration file_path is empty! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( !IsRegularFile(cfg_file) )
	{
		throw Exception(CFG_FILE_INVALID, "The configuration file \"%s\" is not a regular file! [FILE:%s, LINE:%d]", cfg_file.c_str(), __FILE__, __LINE__);
	}

	m_cfgFile = cfg_file;
}

bool Config::RegisterItem(const std::string& segment, const std::string& name)
{
	std::string str_seg  = PubStr::TrimUpperB(segment);
	std::string str_name = PubStr::TrimUpperB(name);

	if ( FindItem(str_seg, str_name) )
	{
		return false;
	}
	
	m_listItems.push_back(CfgItem(str_seg, str_name));
	return true;
}

bool Config::UnregisterItem(const std::string& segment, const std::string& name)
{
	std::string str_seg  = PubStr::TrimUpperB(segment);
	std::string str_name = PubStr::TrimUpperB(name);

	std::list<CfgItem>::iterator it;
	if ( FindItem(str_seg, str_name, &it) )
	{
		m_listItems.erase(it);
		return true;
	}

	return false;
}

void Config::InitItems()
{
	for ( std::list<CfgItem>::iterator it = m_listItems.begin(); it != m_listItems.end(); ++it )
	{
		it->m_value.clear();
		it->m_bFind = false;
	}
}

void Config::DeleteItems()
{
	m_listItems.clear();
}

void Config::ReadConfig() throw(Exception)
{
	if ( m_cfgFile.empty() )
	{
		throw Exception(CFG_FILE_INVALID, "The configuration file unset! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	BaseFile m_bfCfg;
	if ( !m_bfCfg.Open(m_cfgFile) )
	{
		throw Exception(CFG_OPEN_FILE_FAIL, "Open configuration file \"%s\" fail! [FILE:%s, LINE:%d]", m_cfgFile.c_str(), __FILE__, __LINE__);
	}

	std::string strLine;
	std::string strSegment;
	std::string strName;
	std::string strValue;
	while ( m_bfCfg.Read(strLine) )
	{
		CleanComment(strLine);
		PubStr::Trim(strLine);
		if ( strLine.empty() )
		{
			continue;
		}

		if ( TryGetSegment(strLine, strSegment) )
		{
			continue;
		}

		if ( TryGetNameValue(strLine, strName, strValue) )
		{
			std::list<CfgItem>::iterator it;
			if ( FindItem(strSegment, strName, &it) )
			{
				it->m_value = strValue;
				it->m_bFind = true;
			}
		}
	}

	m_bfCfg.Close();
}

std::string Config::GetCfgValue(const std::string& segment, const std::string& name) throw(Exception)
{
	std::string str_seg  = PubStr::TrimUpperB(segment);
	std::string str_name = PubStr::TrimUpperB(name);

	std::list<CfgItem>::iterator it;
	if ( FindItem(str_seg, str_name, &it) )
	{
		if ( !(it->m_bFind) )
		{
			throw Exception(CFG_ITEM_NOT_FOUND, "Configure item [%s->%s] not found! [FILE:%s, LINE:%d]", str_seg.c_str(), str_name.c_str(), __FILE__, __LINE__);
		}
		else if ( it->m_value.empty() )
		{
			throw Exception(CFG_VALUE_INVALID, "Configure item [%s->%s] value is invalid! [FILE:%s, LINE:%d]", str_seg.c_str(), str_name.c_str(), __FILE__, __LINE__);
		}

		return it->m_value;
	}
	else
	{
		throw Exception(CFG_UNREGISTER_ITEM, "Configure item [%s->%s] unregistered! [FILE:%s, LINE:%d]", str_seg.c_str(), str_name.c_str(), __FILE__, __LINE__);
		return std::string();
	}
}

float Config::GetCfgFloatVal(const std::string& segment, const std::string& name)
{
	float f_val = 0.0f;
	PubStr::T1TransT2(GetCfgValue(segment, name), f_val);
	return f_val;
}

long long Config::GetCfgLongVal(const std::string& segment, const std::string& name)
{
	long long ll_val = 0L;
	PubStr::T1TransT2(GetCfgValue(segment, name), ll_val);
	return ll_val;
}

bool Config::GetCfgBoolVal(const std::string& segment, const std::string& name)
{
	std::string str_bool = PubStr::UpperB(GetCfgValue(segment, name));
	return (std::string("TRUE") == str_bool) || (std::string("YES") == str_bool);
}

bool Config::FindItem(const std::string& segment, const std::string& name, std::list<CfgItem>::iterator* pItr /*= NULL*/)
{
	const CfgItem ITEM(segment, name);
	for ( std::list<CfgItem>::iterator it = m_listItems.begin(); it != m_listItems.end(); ++it )
	{
		if ( ITEM == *it )
		{
			if ( pItr != NULL )
			{
				*pItr = it;
			}
			return true;
		}
	}
	
	return false;
}

bool Config::TryGetSegment(const std::string& str, std::string& segment) const
{
	const std::string::size_type SIZE = str.size();
	if ( SIZE <= 2 )
	{
		return false;
	}

	if ( str[0] == '[' && str[SIZE-1] == ']' )
	{
		segment = str.substr(1,SIZE-2);
		PubStr::TrimUpper(segment);
		return true;
	}
	return false;
}

bool Config::TryGetNameValue(const std::string& str, std::string& name, std::string& value) const
{
	const std::string::size_type SIZE = str.size();
	if ( SIZE < 3 )
	{
		return false;
	}

	std::string::size_type equal_pos = str.find('=');
	if ( equal_pos != std::string::npos && equal_pos > 0 )
	{
		name = str.substr(0,equal_pos);
		PubStr::TrimUpper(name);

		value = str.substr(equal_pos+1);
		PubStr::Trim(value);
		return true;
	}
	return false;
}

void Config::CleanComment(std::string& str) const
{
	if ( str.empty() )
	{
		return;
	}

	std::string::size_type first_comm_pos = str.find('#');
	std::string::size_type sec_comm_pos = str.find("//");
	first_comm_pos = first_comm_pos < sec_comm_pos ? first_comm_pos : sec_comm_pos;

	if ( first_comm_pos != std::string::npos )
	{
		str.erase(first_comm_pos);
	}
}

bool Config::IsRegularFile(const std::string& file_path)
{
	struct stat st;
	if ( stat(file_path.c_str(), &st) < 0 )
	{
		return false;
	}

	return S_ISREG(st.st_mode);
}

}	// namespace base

