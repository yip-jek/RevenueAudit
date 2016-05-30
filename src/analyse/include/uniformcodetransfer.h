#pragma once

#include <string>
#include <map>
#include <vector>
#include "exception.h"


// 渠道统一编码信息
struct ChannelUniformCode
{
	std::string ChannelID;				// 统一渠道编码
	std::string ChannelAlias;			// 渠道别名
	std::string ChannelName;			// 渠道中文描述
	std::string Remarks;				// 备注
};

// 地市统一编码信息
struct CityUniformCode
{
	std::string CityID;					// 统一地市编码
	std::string CityAlias;				// 地市别名
	std::string CityName;				// 地市中文描述
	std::string Remarks;				// 备注
};

// 统一编码转换
class UniformCodeTransfer
{
public:
	UniformCodeTransfer();
	~UniformCodeTransfer();

	// 编码转换错误码
	enum UNI_CODE_TRANSFER_ERROR
	{
		UCTERR_INPUT_CHANN_UNICODE_FAILED = -3003001,		// 录入渠道统一编码信息失败
		UCTERR_INPUT_CITY_UNICODE_FAILED  = -3003002,		// 录入地市统一编码信息失败
	};

public:
	// 录入渠道统一编码信息
	void InputChannelUniformCode(std::vector<ChannelUniformCode>& vec_channunicode) throw(base::Exception);

	// 录入地市统一编码信息
	void InputCityUniformCode(std::vector<CityUniformCode>& vec_cityunicode) throw(base::Exception);

	// 编码转换
	bool Transfer(const std::string& src_code, std::string& uni_code);

private:
	std::map<std::string, std::string>	m_mapChannelUniCode;	// 渠道统一编码列表
	std::map<std::string, std::string>	m_mapCityUniCode;		// 地市统一编码列表
};

