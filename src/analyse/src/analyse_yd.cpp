#include "analyse_yd.h"
#include "pubstr.h"
#include "simpletime.h"
#include "log.h"
#include "ydanadb2.h"


const char* const Analyse_YD::S_CHANNEL_MARK = "CHANNEL";			// 渠道标识

Analyse_YD::Analyse_YD()
:m_pYDDB2(NULL)
,m_taskScheLogID(0)
,m_dimEtlDayIndex(0)
,m_dimNowDayIndex(0)
,m_dimRegionIndex(0)
,m_dimChannelIndex(0)
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

	m_pYDDB2->SetTabTaskScheLog(m_tabTaskScheLog);
	m_pYDDB2->SetTabAlarmRequest(m_tabAlarmRequest);

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
		m_pYDDB2->UpdateTaskScheLogState(m_taskScheLogID, base::SimpleTime::Now().Time14(), "ANA_SUCCEED", "分析成功完成", "");
	}
	else	// 分析失败
	{
		// 更新任务日志状态为："ANA_FAILED"（分析失败）
		std::string str_error;
		base::PubStr::SetFormatString(str_error, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
		m_pYDDB2->UpdateTaskScheLogState(m_taskScheLogID, base::SimpleTime::Now().Time14(), "ANA_FAILED", "分析失败", str_error);
	}

	Analyse::End(err_code, err_msg);
}

CAnaDB2* Analyse_YD::CreateDBConnection() throw(base::Exception)
{
	return (m_pYDDB2 = new YDAnaDB2(m_sDBName, m_sUsrName, m_sPasswd));
}

void Analyse_YD::GetExtendParaTaskInfo(VEC_STRING& vec_str) throw(base::Exception)
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
	GetDimIndex();

	const int GROUP_SIZE = m_v3HiveSrcData.size();				// 数据组数目

	Analyse::AnalyseSourceData();

	if ( AnalyseRule::ANATYPE_REPORT_STATISTICS == m_taskInfo.AnaRule.AnaType )		// 报表统计
	{
		// 是否指定渠道？
		// 若指定渠道，则只保留该渠道的报表数据
		VEC_STRING vec_channel;
		if ( TryGetTheChannel(vec_channel) )
		{
			KeepChannelDataOnly(vec_channel);
		}

		// 是否有地市字段？
		if ( m_dimRegionIndex != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在地市维度
		{
			std::map<std::string, std::set<std::string> > mapset_city;
			FilterTheMissingCity(vec_channel, mapset_city);
			MakeCityCompleted(mapset_city, GROUP_SIZE);
		}
	}
}

void Analyse_YD::GetDimIndex()
{
	m_dimEtlDayIndex  = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_ETLDAY);
	m_dimNowDayIndex  = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_NOWDAY);
	m_dimRegionIndex  = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_REGION);
	m_dimChannelIndex = m_taskInfo.GetDimEWTypeIndex(KpiColumn::EWTYPE_CHANNEL);
}

bool Analyse_YD::TryGetTheChannel(VEC_STRING& vec_channel)
{
	std::string exp = base::PubStr::TrimUpperB(m_taskInfo.AnaRule.AnaExpress);
	if ( exp.empty() )
	{
		m_pLog->Output("[Analyse_YD] 没有指定渠道");
		return false;
	}

	VEC_STRING vec_str;
	base::PubStr::Str2StrVector(exp, "=", vec_str);
	if ( vec_str.size() == 2 && S_CHANNEL_MARK == vec_str[0] )
	{
		// 指定多个渠道？(多个渠道间用逗号分隔)
		base::PubStr::Str2StrVector(vec_str[1], ",", vec_str);
		if ( vec_str.empty() )
		{
			m_pLog->Output("[Analyse_YD] 指定渠道: [空]");
			return false;
		}
		else
		{
			const int VEC_SIZE = vec_str.size();
			for ( int i = 0; i < VEC_SIZE; ++i )
			{
				m_pLog->Output("[Analyse_YD] 指定渠道: [%s]", vec_str[i].c_str());
			}

			vec_str.swap(vec_channel);
			return true;
		}
	}

	m_pLog->Output("[Analyse_YD] 指定渠道: [未明]");
	return false;
}

void Analyse_YD::KeepChannelDataOnly(const VEC_STRING& vec_channel)
{
	if ( m_dimChannelIndex != AnaTaskInfo::INVALID_DIM_INDEX )
	{
		VEC2_STRING v2_report;
		const int VEC2_REPORT_SIZE = m_v2ReportStatData.size();
		m_pLog->Output("[Analyse_YD] 原报表数据大小为：%d", VEC2_REPORT_SIZE);

		const int VEC_SIZE = vec_channel.size();
		for ( int i = 0; i < VEC2_REPORT_SIZE; ++i )
		{
			VEC_STRING& ref_vec = m_v2ReportStatData[i];

			for ( int j = 0; j < VEC_SIZE; ++j )
			{
				if ( vec_channel[j] == ref_vec[m_dimChannelIndex] )
				{
					base::PubStr::VVectorSwapPushBack(v2_report, ref_vec);
					break;
				}
			}
		}

		v2_report.swap(m_v2ReportStatData);
		m_pLog->Output("[Analyse_YD] 只保留指定渠道的报表数据后，剩余大小为：%lu", m_v2ReportStatData.size());
	}
}

void Analyse_YD::FilterTheMissingCity(const VEC_STRING& vec_channel, std::map<std::string, std::set<std::string> >& mapset_city)
{
	// 已有地市
	int vec_size = 0;
	std::map<std::string, std::set<std::string> > mapset_ex_city;
	if ( vec_channel.empty() || AnaTaskInfo::INVALID_DIM_INDEX == m_dimChannelIndex )	// 无指定渠道 或者 渠道索引位置无效
	{
		vec_size = m_v2ReportStatData.size();
		for ( int i = 0; i < vec_size; ++i )
		{
			mapset_ex_city[""].insert(m_v2ReportStatData[i][m_dimRegionIndex]);
		}

		m_pLog->Output("[Analyse_YD] 报表数据中，已存在 [%lu] 个地市", mapset_ex_city[""].size());
	}
	else	// 有指定渠道 且 渠道索引位置有效
	{
		// 登记所有指定渠道
		vec_size = vec_channel.size();
		for ( int i = 0; i < vec_size; ++i )
		{
			mapset_ex_city[vec_channel[i]];
		}

		vec_size = m_v2ReportStatData.size();
		for ( int i = 0; i < vec_size; ++i )
		{
			mapset_ex_city[m_v2ReportStatData[i][m_dimChannelIndex]].insert(m_v2ReportStatData[i][m_dimRegionIndex]);
		}

		for ( std::map<std::string, std::set<std::string> >::iterator it = mapset_ex_city.begin(); it != mapset_ex_city.end(); ++it )
		{
			m_pLog->Output("[Analyse_YD] 在渠道 [%s] 的报表数据中，已存在 [%lu] 个地市", it->first.c_str(), it->second.size());
		}
	}

	// 全部地市
	VEC_STRING vec_all_city;
	m_pYDDB2->SelectAllCity(vec_all_city);

	// 找出缺少的地市
	std::string str_chann;
	vec_size = vec_all_city.size();
	for ( std::map<std::string, std::set<std::string> >::iterator it = mapset_ex_city.begin(); it != mapset_ex_city.end(); ++it )
	{
		std::set<std::string>& ref_set = mapset_city[it->first];
		for ( int i = 0; i < vec_size; ++i )
		{
			std::string& ref_vec = vec_all_city[i];
			if ( it->second.find(ref_vec) == it->second.end() )
			{
				ref_set.insert(ref_vec);
			}
		}

		str_chann.clear();
		if ( !it->first.empty() )
		{
			base::PubStr::SetFormatString(str_chann, "渠道 [%s] 的", it->first.c_str());
		}

		m_pLog->Output("[Analyse_YD] 全部共 [%d] 个地市，需要补全%s [%lu] 个地市", vec_size, str_chann.c_str(), ref_set.size());
	}
}

void Analyse_YD::MakeCityCompleted(const std::map<std::string, std::set<std::string> >& mapset_city, int group_size)
{
	if ( !mapset_city.empty() )
	{
		// 初始化：维度列初始为空，值列初始为“0”
		VEC_STRING vec_sup(m_taskInfo.vecKpiDimCol.size(), "");
		vec_sup.insert(vec_sup.end(), group_size, "0");

		// 初始化：时间列
		if ( m_dimEtlDayIndex != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在采集时间
		{
			vec_sup[m_dimEtlDayIndex] = m_dbinfo.GetEtlDay();
		}
		if ( m_dimNowDayIndex != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在当前时间
		{
			vec_sup[m_dimNowDayIndex] = base::SimpleTime::Now().DayTime8();
		}

		std::string str_chann;
		for ( std::map<std::string, std::set<std::string> >::const_iterator c_it = mapset_city.begin(); c_it != mapset_city.end(); ++c_it )
		{
			if ( m_dimChannelIndex != AnaTaskInfo::INVALID_DIM_INDEX )	// 存在渠道
			{
				vec_sup[m_dimChannelIndex] = c_it->first;
			}

			str_chann.clear();
			if ( !c_it->first.empty() )
			{
				base::PubStr::SetFormatString(str_chann, "渠道 [%s] 的", c_it->first.c_str());
			}

			for ( std::set<std::string>::iterator it = c_it->second.begin(); it != c_it->second.end(); ++it )
			{
				vec_sup[m_dimRegionIndex] = *it;
				m_v2ReportStatData.push_back(vec_sup);

				m_pLog->Output("[Analyse_YD] 报表数据补全%s地市: [%s]", str_chann.c_str(), it->c_str());
			}
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

