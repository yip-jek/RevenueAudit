#include "uniformcodetransfer.h"
#include <boost/algorithm/string.hpp>


UniformCodeTransfer::UniformCodeTransfer()
{
}

UniformCodeTransfer::~UniformCodeTransfer()
{
}

void UniformCodeTransfer::InputChannelUniformCode(std::vector<ChannelUniformCode>& vec_channunicode) throw(base::Exception)
{
	m_mapChannelUniCode.clear();

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
		if ( m_mapChannelUniCode.find(ref_chann.ChannelAlias) != m_mapChannelUniCode.end() )
		{
			throw base::Exception(UCTERR_INPUT_CHANN_UNICODE_FAILED, "渠道别名已经存在，重复！[CHANNEL_ID:%s, CHANNEL_ALIAS:%s, CHANNEL_NAME:%s, REMARKS:%s] [FILE:%s, LINE:%d]", ref_chann.ChannelID.c_str(), ref_chann.ChannelAlias.c_str(), ref_chann.ChannelName.c_str(), ref_chann.Remarks.c_str(), __FILE__, __LINE__);
		}

		m_mapChannelUniCode[ref_chann.ChannelAlias] = ref_chann.ChannelID;
	}
}

void UniformCodeTransfer::InputCityUniformCode(std::vector<CityUniformCode>& vec_cityunicode) throw(base::Exception)
{
	m_mapCityUniCode.clear();

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
		if ( m_mapCityUniCode.find(ref_city.CityAlias) != m_mapCityUniCode.end() )
		{
			throw base::Exception(UCTERR_INPUT_CITY_UNICODE_FAILED, "地市别名已经存在，重复！[CITY_ID:%s, CITY_ALIAS:%s, CITY_NAME:%s, REMARKS:%s] [FILE:%s, LINE:%d]", ref_city.CityID.c_str(), ref_city.CityAlias.c_str(), ref_city.CityName.c_str(), ref_city.Remarks.c_str(), __FILE__, __LINE__);
		}

		m_mapCityUniCode[ref_city.CityAlias] = ref_city.CityID;
	}
}

std::string UniformCodeTransfer::Transfer(const std::string& code)
{
	std::string uni_code = code;
	boost::trim(uni_code);

	// 先尝试从渠道统一编码列表中找到统一编码
	std::map<std::string, std::string>::iterator it = m_mapChannelUniCode.find(uni_code);
	if ( it != m_mapChannelUniCode.end() )
	{
		return it->second;
	}

	// 再尝试从地市统一编码列表中找到统一编码
	it = m_mapCityUniCode.find(uni_code);
	if ( it != m_mapCityUniCode.end() )
	{
		return it->second;
	}

	// 找不到则返回原编码
	return uni_code;
}

