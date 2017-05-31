#include "ydalarmmanager.h"
#include "basedir.h"
#include "log.h"
#include "simpletime.h"
#include "pubstr.h"
#include "alarmerror.h"
#include "ydalarmdb.h"
#include "ydstruct.h"

YDAlarmManager::YDAlarmManager()
:m_pAlarmDB(NULL)
{
}

YDAlarmManager::~YDAlarmManager()
{
	ReleaseDBConnection();
}

std::string YDAlarmManager::GetLogFilePrefix()
{
	return ("YDAlarmManager");
}

void YDAlarmManager::Init() throw(base::Exception)
{
	InitDBConnection();

	// 存放告警短信文件的路径是否存在
	if ( !base::BaseDir::IsDirExist(m_alarmMsgFilePath) )
	{
		// 路径不存在，则尝试创建
		if ( !base::BaseDir::CreateFullPath(m_alarmMsgFilePath) )
		{
			throw base::Exception(ALMERR_INIT, "Try to create alarm msg file path failed: PATH=[%s] [FILE:%s, LINE:%d]", m_alarmMsgFilePath.c_str(), __FILE__, __LINE__);
		}

		m_pLog->Output("[YDAlarmManager] Created alarm msg file path: [%s]", m_alarmMsgFilePath.c_str());
	}

	m_pLog->Output("[YDAlarmManager] Init OK.");
}

void YDAlarmManager::LoadExtendedConfig() throw(base::Exception)
{
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_REQUEST");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_THRESHOLD");
	m_cfg.RegisterItem("TABLE", "TAB_ALARM_INFO");
	m_cfg.RegisterItem("TABLE", "TAB_SRC_DATA");

	m_cfg.RegisterItem("ALARM", "ALARM_MSG_PATH");
	m_cfg.RegisterItem("ALARM", "ALARM_MSG_FILE_FMT");

	m_cfg.ReadConfig();

	// 库表配置
	m_tabAlarmRequest   = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_REQUEST");
	m_tabAlarmThreshold = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_THRESHOLD");
	m_tabAlarmInfo      = m_cfg.GetCfgValue("TABLE", "TAB_ALARM_INFO");
	m_tabSrcData        = m_cfg.GetCfgValue("TABLE", "TAB_SRC_DATA");

	m_alarmMsgFilePath   = m_cfg.GetCfgValue("ALARM", "ALARM_MSG_PATH");
	m_alarmMsgFileFormat = m_cfg.GetCfgValue("ALARM", "ALARM_MSG_FILE_FMT");

	m_pLog->Output("[YDAlarmManager] Load configuration OK.");
}

bool YDAlarmManager::ConfirmQuit()
{
	return m_vAlarmReq.empty();
}

void YDAlarmManager::AlarmProcessing() throw(base::Exception)
{
	if ( ResponseAlarmRequest() )
	{
		DataAnalysis();
		GenerateAlarm();
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

bool YDAlarmManager::ResponseAlarmRequest()
{
	m_pAlarmDB->SelectAlarmRequest(m_vAlarmReq);

	const int VEC_SIZE = m_vAlarmReq.size();
	if ( VEC_SIZE > 0 )
	{
		m_pLog->Output("[YDAlarmManager] Get alarm requests: %d", VEC_SIZE);

		for ( int i = 0; i < VEC_SIZE; ++i )
		{
			YDAlarmReq& ref_req = m_vAlarmReq[i];
			m_pLog->Output("[YDAlarmManager] Alarm request [%d]: SEQ=[%d], DATE=[%s], REGION=[%s]", (i+1), ref_req.seq, ref_req.alarm_date.c_str(), ref_req.region.c_str());
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
	MakeAlarmInformation();
	ProduceAlarmMessage();
}

void YDAlarmManager::HandleRequest()
{
	// 清理缓存数据
	std::vector<YDAlarmData>().swap(m_vAlarmData);
	std::vector<YDAlarmData>().swap(m_vAlarmInfo);
}

void YDAlarmManager::UpdateAlarmThreshold()
{
	m_pAlarmDB->SelectAlarmThreshold(m_vAlarmThreshold);
	m_pLog->Output("[YDAlarmManager] Refreshed alarm thresholds: %lu", m_vAlarmThreshold.size());
}

void YDAlarmManager::MakeAlarmInformation()
{
}

void YDAlarmManager::ProduceAlarmMessage()
{
}

void YDAlarmManager::CollectData()
{
	int prev_size = 0;
	int curr_size = 0;

	const int VEC_SIZE = m_vAlarmReq.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YDAlarmReq& ref_req = m_vAlarmReq[i];
		m_pAlarmDB->SelectAlarmData(req.seq, AssembleSQLCondition(ref_req), m_vAlarmData);

		curr_size = m_vAlarmData.size();
		m_pLog->Output("[YDAlarmManager] Collected source data: SEQ=[%d], SIZE=[%d]", (curr_size-prev_size));
		prev_size = curr_size;
	}
}

std::string YDAlarmManager::AssembleSQLCondition(const YDAlarmReq& req)
{
	std::string sql_cond = "DATE = '" + base::PubStr::TrimB(req.alarm_date);
	sql_cond += "' AND MANAGELEVEL = '" + base::PubStr::TrimB(req.region) + "'";

	// 条件：渠道属性
	std::string str_tmp = base::PubStr::TrimB(req.channel_type);
	if ( str_tmp != YDAlarmReq::MARK_ANY )
	{
		sql_cond += " AND CHANNELATTR = '" + str_tmp + "'";
	}

	// 条件：渠道名称
	str_tmp = base::PubStr::TrimB(req.channel_name);
	if ( str_tmp != YDAlarmReq::MARK_ANY )
	{
		sql_cond += " AND CHANNELNAME = '" + str_tmp + "'";
	}

	// 条件：业务分类
	str_tmp = base::PubStr::TrimB(req.busi_type);
	if ( str_tmp != YDAlarmReq::MARK_ANY )
	{
		sql_cond += " AND BUSSORT = '" + str_tmp + "'";
	}

	// 条件：支付方式
	str_tmp = base::PubStr::TrimB(req.pay_type);
	if ( str_tmp != YDAlarmReq::MARK_ANY )
	{
		sql_cond += " AND PAYCODE = '" + str_tmp + "'";
	}

	return sql_cond;
}

void YDAlarmManager::DetermineAlarm()
{
	double threshold = 0.0;
	const int VEC_SIZE = m_vAlarmData.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		YDAlarmData& ref_dat = m_vAlarmData[i];
		if ( GetAlarmThreshold(ref_dat, threshold) )	// 获取到阈值
		{
		}
		else	// 无对应阈值
		{
			m_pLog->Output("[YDAlarmManager] No match alarm threshold: %s", ref_dat.LogPrintInfo().c_str());
		}
	}
}

bool YDAlarmManager::GetAlarmThreshold(const YDAlarmData& alarm_data, double& threshold)
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
		// 渠道名称不匹配
		// 支付方式不匹配

		return true;
	}

	return false;
}

