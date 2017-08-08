#pragma once

class YCFactor_XQB;

// (业财稽核) 详情表因子结果信息
class YCResult_XQB
{
public:
	static const int S_PUBLIC_MEMBERS = 4;			// 公开成员数

	// 结果因子类型
	enum RESULT_FACTOR_TYPE
	{
		RFT_XQB_YCW = 1;			// 详情表（业务侧、财务侧）
		RFT_XQB_GD  = 2;			// 详情表（省）
	};

public:
	YCResult_XQB(RESULT_FACTOR_TYPE type);
	virtual ~YCResult_XQB();

public:
	// 从因子导入
	bool ImportFromFactor(YCFactor_XQB* p_factor);

	// 导入数据
	bool Import(const std::vector<std::string>& vec_dat)
	{
		const int VEC_SIZE = vec_dat.size();
		if ( VEC_SIZE != S_NUMBER_OF_MEMBERS )
		{
			return false;
		}

		int index = 0;
		bill_cyc = vec_dat[index++];
		city     = vec_dat[index++];
		type     = vec_dat[index++];
		dim_id   = vec_dat[index++];
		area     = vec_dat[index++];
		item     = vec_dat[index++];

		if ( !base::PubStr::Str2Int(vec_dat[index++], batch) )
		{
			return false;
		}
	
		value = vec_dat[index++];
		return true;
	}

	// 导出数据
	void Export(std::vector<std::string>& vec_dat) const
	{
		std::vector<std::string> v_dat;
		v_dat.push_back(bill_cyc);
		v_dat.push_back(city);
		v_dat.push_back(type);
		v_dat.push_back(dim_id);
		v_dat.push_back(area);
		v_dat.push_back(item);
		v_dat.push_back(base::PubStr::Int2Str(batch));
		v_dat.push_back(value);

		v_dat.swap(vec_dat);
	}

	std::string LogPrintInfo() const
	{
		std::string info;
		base::PubStr::SetFormatString(info, "BILL_CYC=[%s], "
											"CITY=[%s], "
											"TYPE=[%s], "
											"DIM=[%s], "
											"AREA=[%s], "
											"ITEM=[%s], "
											"BATCH=[%d], "
											"VALUE=[%s]", 
											bill_cyc.c_str(), 
											city.c_str(), 
											type.c_str(), 
											dim_id.c_str(), 
											area.c_str(), 
											item.c_str(), 
											batch, 
											value.c_str());
		return info;
	}

private:
	// 创建详情因子
	void CreateFactor();

	// 释放详情因子
	void ReleaseFactor();

public:
	std::string bill_cyc;				// 账期
	std::string city;					// 地市
	std::string type;					// 类型：0-固定项，1-浮动项
	int         batch;					// 批次

private:
	RESULT_FACTOR_TYPE m_rfType;			// 因子类型
	YCFactor_XQB*      m_pFactor;			// 详情表因子
};

