#pragma once

#include "analyse.h"

// 一点稽核-分析模块
class Analyse_YD : public Analyse
{
public:
	Analyse_YD();
	virtual ~Analyse_YD();

public:
	// 载入参数配置信息
	virtual void LoadConfig() throw(base::Exception);

	// 获取日志文件名称前缀
	virtual std::string GetLogFilePrefix();

	// 初始化
	virtual void Init() throw(base::Exception);

	// 任务结束（资源回收）
	virtual void End(int err_code, const std::string& err_msg = std::string()) throw(base::Exception);

protected:
	// 获取后续参数任务信息
	virtual void GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception);

protected:
	int         m_taskScheLogID;			// 任务日程日志ID
	std::string m_tabTaskScheLog;			// 任务日程日志表
};

