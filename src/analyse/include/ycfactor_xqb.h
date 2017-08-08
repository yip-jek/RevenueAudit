#pragma once

#include <vector>

//struct YCFactor_XQB
//{
//public:
//	static const int S_FACTOR_SIZE = 3;
//
//public:
//	// 导入数据
//	bool Import(const std::vector<std::string>& vec_dat)
//	{
//		const int VEC_SIZE = vec_dat.size();
//		if ( VEC_SIZE != S_FACTOR_SIZE )
//		{
//			return false;
//		}
//
//		int index = 0;
//		area  = vec_dat[index++];
//		item  = vec_dat[index++];
//		value = vec_dat[index++];
//		return true;
//	}
//
//	// 导出数据
//	void Export(std::vector<std::string>& vec_dat) const
//	{
//		std::vector<std::string> v_dat;
//		v_dat.push_back(area);
//		v_dat.push_back(item);
//		v_dat.push_back(value);
//
//		v_dat.swap(vec_dat);
//	}
//
//public:
//	std::string area;					// 区域
//	std::string item;					// 项目内容
//	std::string value;					// 值
//};

// (业财稽核) 详情表因子
class YCFactor_XQB
{
public:
	static const int S_XQB_MEMBERS = 2;			// 成员数

public:
	YCFactor_XQB();
	virtual ~YCFactor_XQB();

public:
	// 导入数据
	virtual bool Import(const std::vector<std::string>& vec_dat) = 0;

	// 导出数据
	virtual void Export(std::vector<std::string>& vec_dat) const = 0;

protected:
	std::string dim_id;					// 维度ID
	std::string area;					// 区域
};

// (业财稽核) 详情表（业务侧、财务侧）因子
class YCFactor_XQB_YCW : public YCFactor_XQB
{
public:
	static const int S_XQBYCW_MEMBERS = 2 + YCFactor_XQB::S_XQB_MEMBERS;		// 成员数

public:
	YCFactor_XQB_YCW();
	virtual ~YCFactor_XQB_YCW();

protected:
	std::string item;					// 项目内容
	std::string value;					// 值
};

// (业财稽核) 详情表（省）因子
class YCFactor_XQB_GD : public YCFactor_XQB
{
public:
	YCFactor_XQB_YCW();
	virtual ~YCFactor_XQB_YCW();

protected:
	std::vector<std::string> vec_items;				// 项目内容（一个或多个）
	std::string              value_YW;				// 业务账
	std::string              value_CW;				// 财务账
};

