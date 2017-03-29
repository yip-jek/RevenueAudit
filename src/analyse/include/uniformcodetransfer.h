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

public:
	// 录入渠道统一编码信息
	void InputChannelUniformCode(std::vector<ChannelUniformCode>& vec_channunicode) throw(base::Exception);

	// 录入地市统一编码信息
	void InputCityUniformCode(std::vector<CityUniformCode>& vec_cityunicode) throw(base::Exception);

	// 地市统一编码转换
	// 输出参数 unicode：成功-返回转换后的统一编码，失败-返回源编码
	bool RegionTransfer(const std::string& src_code, std::string& unicode);

	// 渠道统一编码转换
	// 输出参数 unicode：成功-返回转换后的统一编码，失败-返回源编码
	bool ChannelTransfer(const std::string& src_code, std::string& unicode);

	// 获取地市统一编码中文名
	// 若无法成功获取，则返回源名称
	std::string TryGetRegionCNName(const std::string& unicode);

	// 获取渠道统一编码中文名
	// 若无法成功获取，则返回源名称
	std::string TryGetChannelCNName(const std::string& unicode);

private:
	std::map<std::string, std::string>	m_mapChannelUniCode;	// 渠道统一编码列表
	std::map<std::string, std::string>	m_mapCityUniCode;		// 地市统一编码列表

	std::map<std::string, std::string>	m_mChannUniCodeCN;		// 渠道统一编码中文名列表
	std::map<std::string, std::string>	m_mCityUniCodeCN;		// 地市统一编码中文名列表
};

