#pragma once

#include "ycstatfactor.h"

// （业财）详情表统计因子类
class YCStatFactor_XQB : public YCStatFactor
{
public:
	YCStatFactor_XQB(YCTaskReq& task_req);
	virtual ~YCStatFactor_XQB();

public:
	// 载入因子对
	virtual int LoadFactor(std::vector<std::vector<std::vector<std::string> > >& v3_data) throw(base::Exception);

	// 生成稽核统计结果
	virtual void MakeResult(std::vector<std::vector<std::vector<std::string> > >& v3_result) throw(base::Exception);

private:
};

