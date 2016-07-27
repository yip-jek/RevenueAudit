#include "uniformcodetransfer.h"
#include "pubstr.h"


UniformCodeTransfer::UniformCodeTransfer()
{
}

UniformCodeTransfer::~UniformCodeTransfer()
{
}

void UniformCodeTransfer::InputChannelUniformCode(std::vector<ChannelUniformCode>& vec_channunicode) throw(base::Exception)
{
	m_mapChannelUniCode.clear();
	m_mChannUniCodeCN.clear();

	std::string channel_alias;
	std::string channel_id;
	const size_t VEC_SIZE = vec_channunicode.size();
	for ( size_t i = 0; i < VEC_SIZE; ++i )
	{
		ChannelUniformCode& ref_chann = vec_channunicode[i];

		// 渠道别名为空
		if ( ref_chann.ChannelAlias.empty() )
		{
			throw base::Exception(UCTERR_INPUT_CHANN_UNICODE_FAILED, "渠道别名为空，无效！[CHANNEL_ID:%s, CHANNEL_ALIAS:%s, CHANNEL_NAME:%s, REMARKS:%s] [FILE:%s, LINE:%d]", ref_chann.ChannelID.c_str(), ref_chann.ChannelAlias.c_str(), ref_chann.ChannelName.c_str(), ref_chann.Remarks.c_str(), __FILE__, __LINE__);
		}

		// 统一渠道编码
		if ( ref_chann.ChannelID.empty() )
		{
			throw base::Exception(UCTERR_INPUT_CHANN_UNICODE_FAILED, "统一渠道编码为空，无效！[CHANNEL_ID:%s, CHANNEL_ALIAS:%s, CHANNEL_NAME:%s, REMARKS:%s] [FILE:%s, LINE:%d]", ref_chann.ChannelID.c_str(), ref_chann.ChannelAlias.c_str(), ref_chann.ChannelName.c_str(), ref_chann.Remarks.c_str(), __FILE__, __LINE__);
		}

		// 渠道别名已存在
		channel_alias = base::PubStr::TrimB(ref_chann.ChannelAlias);
		if ( m_mapChannelUniCode.find(channel_alias) != m_mapChannelUniCode.end() )
		{
			throw base::Exception(UCTERR_INPUT_CHANN_UNICODE_FAILED, "渠道别名已经存在，重复！[CHANNEL_ID:%s, CHANNEL_ALIAS:%s, CHANNEL_NAME:%s, REMARKS:%s] [FILE:%s, LINE:%d]", ref_chann.ChannelID.c_str(), ref_chann.ChannelAlias.c_str(), ref_chann.ChannelName.c_str(), ref_chann.Remarks.c_str(), __FILE__, __LINE__);
		}

		m_mapChannelUniCode[channel_alias]     = ref_chann.ChannelID;

		// 是否为新的渠道统一编码中文名？
		channel_id = base::PubStr::TrimB(ref_chann.ChannelID);
		if ( m_mChannUniCodeCN.find(channel_id) == m_mChannUniCodeCN.end() )
		{
			m_mChannUniCodeCN[channel_id] = ref_chann.ChannelName;
		}
	}
}

void UniformCodeTransfer::InputCityUniformCode(std::vector<CityUniformCode>& vec_cityunicode) throw(base::Exception)
{
	m_mapCityUniCode.clear();
	m_mCityUniCodeCN.clear();

	std::string city_alias;
	std::string city_id;
	const size_t VEC_SIZE = vec_cityunicode.size();
	for ( size_t i = 0; i < VEC_SIZE; ++i )
	{
		CityUniformCode& ref_city = vec_cityunicode[i];

		// 地市别名为空
		if ( ref_city.CityAlias.empty() )
		{
			throw base::Exception(UCTERR_INPUT_CITY_UNICODE_FAILED, "地市别名为空，无效！[CITY_ID:%s, CITY_ALIAS:%s, CITY_NAME:%s, REMARKS:%s] [FILE:%s, LINE:%d]", ref_city.CityID.c_str(), ref_city.CityAlias.c_str(), ref_city.CityName.c_str(), ref_city.Remarks.c_str(), __FILE__, __LINE__);
		}

		// 统一地市编码
		if ( ref_city.CityID.empty() )
		{
			throw base::Exception(UCTERR_INPUT_CITY_UNICODE_FAILED, "统一地市编码为空，无效！[CITY_ID:%s, CITY_ALIAS:%s, CITY_NAME:%s, REMARKS:%s] [FILE:%s, LINE:%d]", ref_city.CityID.c_str(), ref_city.CityAlias.c_str(), ref_city.CityName.c_str(), ref_city.Remarks.c_str(), __FILE__, __LINE__);
		}

		// 地市别名已存在
		// 地市别名以大写形式保存！
		city_alias = base::PubStr::TrimUpperB(ref_city.CityAlias);
		if ( m_mapCityUniCode.find(city_alias) != m_mapCityUniCode.end() )
		{
			throw base::Exception(UCTERR_INPUT_CITY_UNICODE_FAILED, "地市别名已经存在，重复！[CITY_ID:%s, CITY_ALIAS:%s, CITY_NAME:%s, REMARKS:%s] [FILE:%s, LINE:%d]", ref_city.CityID.c_str(), ref_city.CityAlias.c_str(), ref_city.CityName.c_str(), ref_city.Remarks.c_str(), __FILE__, __LINE__);
		}

		m_mapCityUniCode[city_alias] = ref_city.CityID;

		// 是否为新的地市统一编码中文名？
		city_id = base::PubStr::TrimUpperB(ref_city.CityID);
		if ( m_mCityUniCodeCN.find(city_id) == m_mCityUniCodeCN.end() )
		{
			m_mCityUniCodeCN[city_id] = ref_city.CityName;
		}
	}
}

bool UniformCodeTransfer::RegionTransfer(const std::string& src_code, std::string& unicode)
{
	// 地市别名需大写
	const std::string CODE = base::PubStr::TrimUpperB(src_code);

	// 从地市统一编码列表中找到统一编码
	std::map<std::string, std::string>::iterator it = m_mapCityUniCode.find(CODE);
	if ( it != m_mapCityUniCode.end() )
	{
		unicode = it->second;
		return true;
	}

	// 找不到，则返回源编码
	unicode = CODE;
	return false;
}

bool UniformCodeTransfer::ChannelTransfer(const std::string& src_code, std::string& unicode)
{
	const std::string CODE = base::PubStr::TrimB(src_code);

	// 从渠道统一编码列表中找到统一编码
	std::map<std::string, std::string>::iterator it = m_mapChannelUniCode.find(CODE);
	if ( it != m_mapChannelUniCode.end() )
	{
		unicode = it->second;
		return true;
	}

	// 找不到，则返回源编码
	unicode = CODE;
	return false;
}

std::string UniformCodeTransfer::TryGetRegionCNName(const std::string& unicode)
{
	// 地市统一编码需大写
	const std::string UNI_CODE = base::PubStr::TrimUpperB(unicode);

	// 从地市统一编码中文名列表中找到中文名
	std::map<std::string, std::string>::iterator it = m_mCityUniCodeCN.find(UNI_CODE);
	if ( it != m_mCityUniCodeCN.end() )
	{
		return it->second;
	}

	// 找不到，返回源名称
	return UNI_CODE;
}

std::string UniformCodeTransfer::TryGetChannelCNName(const std::string& unicode)
{
	const std::string UNI_CODE = base::PubStr::TrimB(unicode);

	// 从渠道统一编码中文名列表中找到中文名
	std::map<std::string, std::string>::iterator it = m_mChannUniCodeCN.find(UNI_CODE);
	if ( it != m_mChannUniCodeCN.end() )
	{
		return it->second;
	}

	// 找不到，返回源名称
	return UNI_CODE;
}

