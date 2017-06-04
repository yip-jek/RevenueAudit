#include "ydalarmmanager.h"
#include "basedir.h"
#include "log.h"
#include "simpletime.h"
#include "pubstr.h"
#include "pubtime.h"
#include "sqltranslator.h"
#include "alarmerror.h"
#include "ydalarmdb.h"
#include "ydalarmfile.h"
#include "ydstruct.h"

YDAlarmManager::YDAlarmManager()
:m_totalAlarmRequests(0)
,m_alarmShowTime(0)
,m_stAlarmShow(0)
,m_alarmMsgFileMaxLine(0)
,m_pAlarmDB(NULL)
,m_pAlarmSMSFile(NULL)
{
}

YDAlarmManager::~YDAlarmManager()
{
	ReleaseAlarmSMSFile();
	ReleaseDBConnection();
}

std::string YDAlarmManager::GetLogFilePrefix()
{
	return ("YDAlarmManager");
}

void YDAlarmManager::Init() throw(base::Exception)
{
	AlarmManager::Init();

	InitAlarmShowTime();
	InitDBConnection();
	InitAlarmSMSFile();

	m_pLog->Output("[YDAlarmManager] Init OK.");
}

void YDAlarmManager::LoadExtendedConfig() throw(base::Exception)
{
	m_cfg.RegisterItem("SYS", "ALARM_SHOW_TIME");

	m_cfg.RegisterItem("TABLE", "TAB_ALARM_REQUEST");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_THRESHOLD");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_INFO");
	m_cfg.RegisterItem("TABLE", "TAB_SRC_DATA");

	m_cfg.RegisterItem("ALARM", "ALARM_MSG_PATH");
	m_cfg.RegisterItem("ALARM", "ALARM_MSG_FILE_FMT");
	m_cfg.RegisterItem("ALARM", "ALARM_MSG_FILE_MAX_LINE");

	m_cfg.ReadConfig();

	// 告警日志输出时间配置
	m_alarmShowTime = m_cfg.GetCfgLongVal("SYS", "ALARM_SHOW_TIME");

	// 库表配置
	m_tabAlarmRequest   = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_REQUEST");
	m_tabAlarmThreshold = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_THRESHOLD");
	m_tabAlarmInfo      = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_INFO");
	m_tabSrcData        = m_cfg.GetCfgValue("TABLE", "TAB_SRC_DATA");

	// 告警短信文件配置
	m_alarmMsgFilePath    = m_cfg.GetCfgValue("ALARM", "ALARM_MSG_PATH");
	m_alarmMsgFileFormat  = m_cfg.GetCfgValue("ALARM", "ALARM_MSG_FILE_FMT");
	m_alarmMsgFileMaxLine = m_cfg.GetCfgLongVal("ALARM", "ALARM_MSG_FILE_MAX_LINE");

	m_pLog->Output("[YDAlarmManager] Load configuration OK.");
}

void YDAlarmManager::OutputExtendedConfig()
{
	m_pLog->Output(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
	m_pLog->Output("[YDAlarmManager] (EXTENDED_CONFIG) [SYS]->[ALARM_SHOW_TIME]          : [%d]", m_alarmShowTime);
	m_pLog->Output("[YDAlarmManager] (EXTENDED_CONFIG) [TABLE]->[TAB_ALARM_REQUEST]      : [%s]", m_tabAlarmRequest.c_str());
	m_pLog->Output("[YDAlarmManager] (EXTENDED_CONFIG) [TABLE]->[TAB_ALARM_THRESHOLD]    : [%s]", m_tabAlarmThreshold.c_str());
	m_pLog->Output("[YDAlarmManager] (EXTENDED_CONFIG) [TABLE]->[TAB_ALARM_INFO]         : [%s]", m_tabAlarmInfo.c_str());
	m_pLog->Output("[YDAlarmManager] (EXTENDED_CONFIG) [TABLE]->[TAB_SRC_DATA]           : [%s]", m_tabSrcData.c_str());
	m_pLog->Output("[YDAlarmManager] (EXTENDED_CONFIG) [ALARM]->[ALARM_MSG_PATH]         : [%s]", m_alarmMsgFilePath.c_str());
	m_pLog->Output("[YDAlarmManager] (EXTENDED_CONFIG) [ALARM]->[ALARM_MSG_FILE_FMT]     : [%s]", m_alarmMsgFileFormat.c_str());
	m_pLog->Output("[YDAlarmManager] (EXTENDED_CONFIG) [ALARM]->[ALARM_MSG_FILE_MAX_LINE]: [%d]", m_alarmMsgFileMaxLine);
	m_pLog->Output("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
}

bool YDAlarmManager::ConfirmQuit()
{
	return m_mAlarmReq.empty();
}

void YDAlarmManager::AlarmProcessing() throw(base::Exception)
{
	if ( ResponseAlarmRequest() )
	{
		DataAnalysis();

		GenerateAlarm();

		ShowAlarmState();

		HandleRequest();
	}
}

void YDAlarmManager::ReleaseDBConnection()
{
	if ( m_pAlarmDB != NULL )
	{
		delete m_pAlarmDB;
		m_pAlarmDB = NULL;
	}
}

void YDAlarmManager::ReleaseAlarmSMSFile()
{
	if ( m_pAlarmSMSFile != NULL )
	{
		delete m_pAlarmSMSFile;
		m_pAlarmSMSFile = NULL;
	}
}

void YDAlarmManager::InitAlarmShowTime() throw(base::Exception)
{
	if ( m_alarmShowTime <= 0 )
	{
		throw base::Exception(ALMERR_INIT_ALARM_SHOW, "The alarm show time is invalid: %d [FILE:%s, LINE:%d]", m_alarmShowTime, __FILE__, __LINE__);
	}

	if ( !m_stAlarmShow.Set(m_alarmShowTime) )
	{
		throw base::Exception(ALMERR_INIT_ALARM_SHOW, "Set the alarm show timer failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	if ( !m_stAlarmShow.Start() )
	{
		throw base::Exception(ALMERR_INIT_ALARM_SHOW, "Turn on the alarm show timer failed! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}
}

void YDAlarmManager::InitDBConnection() throw(base::Exception)
{
	ReleaseDBConnection();

	m_pAlarmDB = new YDAlarmDB(m_dbinfo);
	if ( NULL == m_pAlarmDB )
	{
		throw base::Exception(ALMERR_INIT_DB_CONN, "Operator new YDAlarmDB failed: 无法申请到内存空间！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pAlarmDB->Connect();
	m_pAlarmDB->SetTabAlarmRequest(m_tabAlarmRequest);
	m_pAlarmDB->SetTabAlarmThreshold(m_tabAlarmThreshold);
	m_pAlarmDB->SetTabAlarmInfo(m_tabAlarmInfo);
	m_pAlarmDB->SetTabSrcData(m_tabSrcData);
}

void YDAlarmManager::InitAlarmSMSFile() throw(base::Exception)
{
	ReleaseAlarmSMSFile();

	m_pAlarmSMSFile = new YDAlarmFile();
	if ( NULL == m_pAlarmSMSFile )
	{
		throw base::Exception(ALMERR_INIT_ALARM_FILE, "Operator new YDAlarmFile failed: 无法申请到内存空间！[FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_pAlarmSMSFile->SetPath(m_alarmMsgFilePath);
	m_pAlarmSMSFile->SetFileFormat(m_alarmMsgFileFormat);
	m_pAlarmSMSFile->SetMaxLine(m_alarmMsgFileMaxLine);
}

bool YDAlarmManager::ResponseAlarmRequest()
{
	m_pAlarmDB->SelectAlarmRequest(m_mAlarmReq);

	if ( !m_mAlarmReq.empty() )
	{
		m_pLog->Output("[YDAlarmManager] Get alarm requests: %lu", m_mAlarmReq.size());

		std::map<int, YDAlarmReq>::iterator it = m_mAlarmReq.begin();
		for ( int i = 1; it != m_mAlarmReq.end(); ++it )
		{
			m_pLog->Output("[YDAlarmManager] Alarm request [%d]: %s", (i++), it->second.LogPrintInfo().c_str());
		}

		return true;
	}

	return false;
}

void YDAlarmManager::DataAnalysis()
{
	UpdateAlarmThreshold();

	CollectData();

	DetermineAlarm();
}

void YDAlarmManager::GenerateAlarm()
{
	m_pAlarmSMSFile->OpenNewAlarmFile();

	std::string alarm_msg;
	const int VEC_SIZE = m_vAlarmInfo.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YDAlarmInfo& ref_info = m_vAlarmInfo[i];

		m_pLog->Output("[YDAlarmManager] 登记告警信息 (%d): %s", (i+1), ref_info.LogPrintInfo().c_str());
		m_pAlarmDB->InsertAlarmInfo(ref_info);

		alarm_msg = ProduceAlarmMessage(ref_info);
		m_pAlarmSMSFile->WriteAlarmData(alarm_msg);
	}

	m_pLog->Output("[YDAlarmManager] 总共写入告警信息 %d 条.", VEC_SIZE);
	m_pAlarmSMSFile->CloseAlarmFile();
}

void YDAlarmManager::ShowAlarmState()
{
	if ( m_stAlarmShow.IsTimeUp() )
	{
		m_pLog->Output("####################################################");
		m_pLog->Output("[YDAlarmManager] ALARM REQUEST SIZE      ：[%lu]", m_mAlarmReq.size());
		m_pLog->Output("[YDAlarmManager] ALARM THRESHOLD SIZE    ：[%lu]", m_vAlarmThreshold.size());
		m_pLog->Output("[YDAlarmManager] ALARM DATA SIZE         ：[%lu]", m_vAlarmData.size());
		m_pLog->Output("[YDAlarmManager] ALARM INFO SIZE         ：[%lu]", m_vAlarmInfo.size());
		m_pLog->Output("[YDAlarmManager] TOTAL ALARM REQUEST SIZE：[%d]" , m_totalAlarmRequests);
		m_pLog->Output("####################################################");
	}
}

void YDAlarmManager::HandleRequest()
{
	UpdateRequestStatus();

	// 记录告警请求数
	m_totalAlarmRequests += m_mAlarmReq.size();

	// 清理缓存数据
	std::vector<YDAlarmData>().swap(m_vAlarmData);
	std::vector<YDAlarmInfo>().swap(m_vAlarmInfo);
}

void YDAlarmManager::UpdateAlarmThreshold()
{
	m_pAlarmDB->SelectAlarmThreshold(m_vAlarmThreshold);
	m_pLog->Output("[YDAlarmManager] Refreshed alarm thresholds: %lu", m_vAlarmThreshold.size());
}

std::string YDAlarmManager::ProduceAlarmMessage(const YDAlarmInfo& alarm_info)
{
	std::string sms_msg = alarm_info.msg_template;
	std::string mark;
	size_t      pos = 0;

	// 短信模板（举例）：
	// $(ALARM_DATE)，$(REGION)$(CHANN_NAME)$(PAY_TYPE)产生欠费$(ARREARS)（元），请及时追缴！
	while ( base::SQLTranslator::GetMark(sms_msg, mark, pos) )
	{
		mark = alarm_info.GetInfoByMark(mark);
		sms_msg.replace(pos, mark.size()+3, mark);
	}

	// 短信记录格式：短信接收号码|发送短信文本信息
	std::string alarm_msg = base::PubStr::TrimB(alarm_info.call_no) + "|" + sms_msg;
	return alarm_msg;
}

void YDAlarmManager::CollectData()
{
	std::string sql_cond;
	int prev_size = 0;
	int curr_size = 0;
	int diff_size = 0;

	for ( std::map<int, YDAlarmReq>::iterator it = m_mAlarmReq.begin(); it != m_mAlarmReq.end(); ++it )
	{
		YDAlarmReq& ref_req = it->second;
		sql_cond = AssembleSQLCondition(ref_req);
		m_pAlarmDB->SelectAlarmData(ref_req.seq, sql_cond, m_vAlarmData);

		curr_size = m_vAlarmData.size();
		diff_size = curr_size - prev_size;
		m_pLog->Output("[YDAlarmManager] Collected source data: SEQ=[%d], SIZE=[%d]", it->first, diff_size);

		// 更新告警请求状态：采集不到告警源数据，状态异常
		if ( diff_size <= 0 )
		{
			m_pLog->Output("[YDAlarmManager] 采集不到告警源数据，告警请求状态异常：SEQ=[%d]", it->first);
			ref_req.status      = YDAlarmReq::RS_Error;
			ref_req.finish_time = base::SimpleTime::Now().Time14();
		}

		prev_size = curr_size;
	}

	m_pLog->Output("[YDAlarmManager] Collected all source data size: [%d]", curr_size);
}

std::string YDAlarmManager::AssembleSQLCondition(const YDAlarmReq& req)
{
	std::string sql_cond = "DATE = '" + base::PubStr::TrimB(req.alarm_date);
	sql_cond += "' AND MANAGELEVEL = '" + base::PubStr::TrimB(req.region) + "'";

	// 条件：渠道属性
	std::string str_tmp = base::PubStr::TrimB(req.channel_type);
	if ( str_tmp != YDAlarmReq::S_MARK_ANY )
	{
		sql_cond += " AND CHANNELATTR = '" + str_tmp + "'";
	}

	// 条件：渠道名称
	str_tmp = base::PubStr::TrimB(req.channel_name);
	if ( str_tmp != YDAlarmReq::S_MARK_ANY )
	{
		sql_cond += " AND CHANNELNAME = '" + str_tmp + "'";
	}

	// 条件：业务分类
	str_tmp = base::PubStr::TrimB(req.busi_type);
	if ( str_tmp != YDAlarmReq::S_MARK_ANY )
	{
		sql_cond += " AND BUSSORT = '" + str_tmp + "'";
	}

	// 条件：支付方式
	str_tmp = base::PubStr::TrimB(req.pay_type);
	if ( str_tmp != YDAlarmReq::S_MARK_ANY )
	{
		sql_cond += " AND PAYCODE = '" + str_tmp + "'";
	}

	return sql_cond;
}

void YDAlarmManager::DetermineAlarm()
{
	YDAlarmThreshold* pAlarmThreshold = NULL;
	const int VEC_SIZE = m_vAlarmData.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YDAlarmData& ref_dat = m_vAlarmData[i];
		if ( GetAlarmThreshold(ref_dat, pAlarmThreshold) )	// 获取到阈值
		{
			// 是否达到告警阈值
			if ( IsReachThreshold(ref_dat, *pAlarmThreshold) )
			{
				m_pLog->Output("[YDAlarmManager] Reached alarm threshold [%s]: %s", base::PubStr::Double2FormatStr(pAlarmThreshold->threshold).c_str(), ref_dat.LogPrintInfo().c_str());
				MakeAlarmInformation(ref_dat, *pAlarmThreshold);
			}
		}
		else	// 无对应阈值
		{
			m_pLog->Output("[YDAlarmManager] No match alarm threshold: %s", ref_dat.LogPrintInfo().c_str());

			YDAlarmReq& ref_req = m_mAlarmReq[ref_dat.seq];
			if ( ref_req.status != YDAlarmReq::RS_Error )
			{
				m_pLog->Output("[YDAlarmManager] 无对应匹配的告警阈值，告警请求状态异常：SEQ=[%d]", ref_req.seq);
				ref_req.status      = YDAlarmReq::RS_Error;
				ref_req.finish_time = base::SimpleTime::Now().Time14();
			}
		}
	}

	m_pLog->Output("[YDAlarmManager] Maked alarm info size: [%lu]", m_vAlarmInfo.size());
}

bool YDAlarmManager::GetAlarmThreshold(const YDAlarmData& alarm_data, YDAlarmThreshold*& pThreshold)
{
	const int VEC_SIZE = m_vAlarmThreshold.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YDAlarmThreshold& ref_threshold = m_vAlarmThreshold[i];

		// 地市不匹配
		if ( alarm_data.manage_level != ref_threshold.region )
		{
			continue;
		}

		// 渠道属性不匹配
		if ( ref_threshold.channel_type != YDAlarmReq::S_MARK_ANY 
			&& alarm_data.channel_attr != ref_threshold.channel_type )
		{
			continue;
		}

		// 渠道名称不匹配
		if ( ref_threshold.channel_name != YDAlarmReq::S_MARK_ANY 
			&& alarm_data.channel_name != ref_threshold.channel_name )
		{
			continue;
		}

		// 支付方式不匹配
		if ( ref_threshold.pay_type != YDAlarmReq::S_MARK_ANY 
			&& alarm_data.pay_code != ref_threshold.pay_type )
		{
			continue;
		}

		pThreshold = &ref_threshold;
		return true;
	}

	pThreshold = NULL;
	return false;
}

bool YDAlarmManager::IsReachThreshold(const YDAlarmData& alarm_data, const YDAlarmThreshold& alarm_threshold)
{
	return (alarm_data.arrears >= alarm_threshold.threshold);
}

void YDAlarmManager::MakeAlarmInformation(const YDAlarmData& alarm_data, const YDAlarmThreshold& alarm_threshold)
{
	YDAlarmInfo alarm_info;
	alarm_info.seq           = alarm_data.seq;
	alarm_info.alarm_date    = alarm_data.alarm_date;
	alarm_info.region        = alarm_data.manage_level;
	alarm_info.channel_type  = alarm_data.channel_attr;
	alarm_info.channel_name  = alarm_data.channel_name;
	alarm_info.busi_type     = alarm_data.bus_sort;
	alarm_info.pay_type      = alarm_data.pay_code;
	alarm_info.responser     = alarm_threshold.responser;
	alarm_info.call_no       = alarm_threshold.call_no;
	alarm_info.generate_time = base::SimpleTime::Now().Time14();
	alarm_info.msg_template  = alarm_threshold.msg_template;
	alarm_info.arrears       = alarm_data.arrears;

	// 告警计划完成时间：告警生成时间+计划完成日期偏移量
	alarm_info.plan_time  = base::PubTime::DateNowPlusDays(alarm_threshold.offset);
	alarm_info.plan_time += alarm_info.generate_time.substr(8);

	m_vAlarmInfo.push_back(alarm_info);
}

void YDAlarmManager::UpdateRequestStatus()
{
	// 更新告警请求状态
	for ( std::map<int, YDAlarmReq>::iterator it = m_mAlarmReq.begin(); it != m_mAlarmReq.end(); ++it )
	{
		YDAlarmReq& ref_req = it->second;

		// 若状态不为异常，则设置为完成
		if ( ref_req.status != YDAlarmReq::RS_Error )
		{
			ref_req.status      = YDAlarmReq::RS_Finish;
			ref_req.finish_time = base::SimpleTime::Now().Time14();
		}

		m_pAlarmDB->UpdateAlarmRequest(ref_req);
	}
}

