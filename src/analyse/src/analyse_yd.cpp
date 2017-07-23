#include "analyse_yd.h"
#include "pubstr.h"
#include "simpletime.h"
#include "log.h"
#include "canadb2.h"
#include "taskinfoutil.h"

const char* const Analyse_YD::S_CHANNEL_MARK = "CHANNEL";			// 渠道标识

Analyse_YD::Analyse_YD()
:m_taskScheLogID(0)
{
	m_sType = "一点稽核";
}

Analyse_YD::~Analyse_YD()
{
}

std::string Analyse_YD::GetLogFilePrefix()
{
	return std::string("Analyse_YD");
}

void Analyse_YD::LoadConfig() throw(base::Exception)
{
	Analyse::LoadConfig();

	m_cfg.RegisterItem("TABLE", "TAB_TASK_SCHE_LOG");		// 读取任务日程日志表配置
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_REQUEST");

	m_cfg.ReadConfig();

	m_tabTaskScheLog  = m_cfg.GetCfgValue("TABLE", "TAB_TASK_SCHE_LOG");
	m_tabAlarmRequest = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_REQUEST");

	m_pLog->Output("[Analyse_YD] Load configuration OK.");
}

void Analyse_YD::Init() throw(base::Exception)
{
	Analyse::Init();

	m_pAnaDB2->SetTabTaskScheLog(m_tabTaskScheLog);
	m_pAnaDB2->SetTabAlarmRequest(m_tabAlarmRequest);

	m_pLog->Output("[Analyse_YD] Init OK.");
}

void Analyse_YD::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	// 更新任务日程日志状态
	if ( 0 == err_code )	// 分析成功
	{
		// 只有分析成功才生成告警请求
		AlarmRequest();

		// 更新任务日志状态为："ANA_SUCCEED"（分析成功）
		m_pAnaDB2->UpdateTaskScheLogState(m_taskScheLogID, base::SimpleTime::Now().Time14(), "ANA_SUCCEED", "分析成功完成", "");
	}
	else	// 分析失败
	{
		// 更新任务日志状态为："ANA_FAILED"（分析失败）
		std::string str_error;
		base::PubStr::SetFormatString(str_error, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
		m_pAnaDB2->UpdateTaskScheLogState(m_taskScheLogID, base::SimpleTime::Now().Time14(), "ANA_FAILED", "分析失败", str_error);
	}

	Analyse::End(err_code, err_msg);
}

void Analyse_YD::GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < 4 )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法拆分出任务日程日志ID! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	std::string& ref_str = vec_str[3];
	if ( !base::PubStr::Str2Int(ref_str, m_taskScheLogID) )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "无效的任务日程日志ID：%s [FILE:%s, LINE:%d]", ref_str.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Analyse_YD] 任务日程日志ID：%d", m_taskScheLogID);
}

void Analyse_YD::AnalyseSourceData() throw(base::Exception)
{
	const int VAL_COL_SIZE = m_v3HiveSrcData.size();								// 值列的数目
	const int DIM_COL_SIZE = TaskInfoUtil::TheFirstDataColSize(m_v3HiveSrcData) - 1;// 维度列的数目

	Analyse::AnalyseSourceData();

	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == m_taskInfo.AnaRule.AnaType )		// 报表统计
	{
		// 是否指定渠道？
		// 若指定渠道，则只保留该渠道的报表数据
		const std::string THE_CHANNEL = TryGetTheChannel();
		KeepChannelDataOnly(THE_CHANNEL);

		// 是否有地市字段？
		const int DIM_REGION_INDEX  = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_REGION);
		if ( DIM_REGION_INDEX != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在地市维度
		{
			std::set<std::string> set_city;
			FilterTheMissingCity(set_city, DIM_REGION_INDEX);
			MakeCityCompleted(set_city, THE_CHANNEL, DIM_COL_SIZE, VAL_COL_SIZE);
		}
	}
}

std::string Analyse_YD::TryGetTheChannel()
{
	std::string exp = base::PubStr::TrimUpperB(m_taskInfo.AnaRule.AnaExpress);
	if ( exp.empty() )
	{
		m_pLog->Output("[Analyse_YD] 没有指定渠道");
		return "";
	}

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(exp, "=", vec_str);
	if ( vec_str.size() == 2 && S_CHANNEL_MARK == vec_str[0] )
	{
		m_pLog->Output("[Analyse_YD] 指定渠道: [%s]", vec_str[1].c_str());
		return vec_str[1];
	}

	m_pLog->Output("[Analyse_YD] 指定渠道: [未明]");
	return "";
}

void Analyse_YD::KeepChannelDataOnly(const std::string& channel)
{
	const int DIM_CHANNEL_INDEX  = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_CHANNEL);
	if ( !channel.empty() && DIM_CHANNEL_INDEX != AnaTaskInfo::INVALID_DIM_INDEX )
	{
		std::vector<std::vector<std::string> > v2_report;
		const int VEC2_REPORT_SIZE = m_v2ReportStatData.size();
		m_pLog->Output("[Analyse_YD] 原报表数据大小为：%d", VEC2_REPORT_SIZE);

		for ( int i = 0; i < VEC2_REPORT_SIZE; ++i )
		{
			std::vector<std::string>& ref_vec = m_v2ReportStatData[i];

			if ( channel == ref_vec[DIM_CHANNEL_INDEX] )
			{
				base::PubStr::VVectorSwapPushBack(v2_report, ref_vec);
			}
		}

		v2_report.swap(m_v2ReportStatData);
		m_pLog->Output("[Analyse_YD] 只保留指定渠道 [%s] 报表数据后，剩余大小为：%llu", channel.c_str(), m_v2ReportStatData.size());
	}
}

void Analyse_YD::FilterTheMissingCity(std::set<std::string>& set_city, int dim_region_index)
{
	// 已有地市
	std::set<std::string> set_ex_city;
	int vec_size = m_v2ReportStatData.size();
	for ( int i = 0; i < vec_size; ++i )
	{
		set_ex_city.insert(m_v2ReportStatData[i][dim_region_index]);
	}
	m_pLog->Output("[Analyse_YD] 报表数据中，已存在 %d 个地市", (int)set_ex_city.size());

	// 全部地市
	std::vector<std::string> vec_all_city;
	m_pAnaDB2->SelectAllCity(vec_all_city);

	// 找出缺少的地市
	set_city.clear();
	vec_size = vec_all_city.size();
	for ( int j = 0; j < vec_size; ++j )
	{
		std::string& ref_vec = vec_all_city[j];
		if ( set_ex_city.find(ref_vec) == set_ex_city.end() )
		{
			set_city.insert(ref_vec);
		}
	}
	m_pLog->Output("[Analyse_YD] 全部地市共 %d 个，需要补全 %d 个地市", vec_size, (int)set_city.size());
}

void Analyse_YD::MakeCityCompleted(const std::set<std::string>& set_city, const std::string& channel, int dim_size, int val_size)
{
	if ( !set_city.empty() )
	{
		const int DIM_ETLDAY_INDEX = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_ETLDAY);
		const int DIM_NOWDAY_INDEX = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_NOWDAY);
		const int DIM_REGION_INDEX = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_REGION);
		const int DIM_CHANN_INDEX  = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_CHANNEL);

		// 初始化：维度列初始为空，值列初始为“0”
		std::vector<std::string> vec_sup(dim_size, "");
		vec_sup.insert(vec_sup.end(), val_size, "0");

		// 初始化：时间列、渠道列
		if ( DIM_ETLDAY_INDEX != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在采集时间
		{
			vec_sup[DIM_ETLDAY_INDEX] = m_dbinfo.GetEtlDay();
		}
		if ( DIM_NOWDAY_INDEX != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在当前时间
		{
			vec_sup[DIM_NOWDAY_INDEX] = base::SimpleTime::Now().DayTime8();
		}
		if ( DIM_CHANN_INDEX != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在渠道
		{
			vec_sup[DIM_CHANN_INDEX] = channel;
		}

		for ( std::set<std::string>::iterator it = set_city.begin(); it != set_city.end(); ++it )
		{
			vec_sup[DIM_REGION_INDEX] = *it;
			m_v2ReportStatData.push_back(vec_sup);

			m_pLog->Output("[Analyse_YD] 报表数据补全地市: [%s]", it->c_str());
		}
	}
}

void Analyse_YD::AlarmRequest()
{
	// >>>>> 告警标记 <<<<<
	// ALARM_REQ: 告警请求
	std::string alarm_flag = base::PubStr::TrimUpperB(m_taskInfo.AlarmID);
	if ( "ALARM_REQ" == alarm_flag )
	{
	}
}

