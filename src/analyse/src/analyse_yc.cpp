#include "analyse_yc.h"
#include "log.h"
#include "pubstr.h"
#include "simpletime.h"
#include "canadb2.h"


Analyse_YC::Analyse_YC()
:m_ycSeqID(0)
{
	m_sType = "业财稽核";
}

Analyse_YC::~Analyse_YC()
{
}

void Analyse_YC::LoadConfig() throw(base::Exception)
{
	Analyse::LoadConfig();

	m_cfg.RegisterItem("TABLE", "TAB_YC_TASK_REQ");
	m_cfg.ReadConfig();
	m_tabYCTaskReq = m_cfg.GetCfgValue("TABLE", "TAB_YC_TASK_REQ");

	m_pLog->Output("[Analyse_YC] Load configuration OK.");
}

std::string Analyse_YC::GetLogFilePrefix()
{
	return std::string("Analyse_YC");
}

void Analyse_YC::Init() throw(base::Exception)
{
	Analyse::Init();

	m_pAnaDB2->SetTabYCTaskReq(m_tabYCTaskReq);

	// 更新任务状态为；"21"（正在分析）
	m_pAnaDB2->UpdateYCTaskReq(m_ycSeqID, "21", "正在分析", "分析开始时间："+base::SimpleTime::Now().TimeStamp());

	m_pLog->Output("[Analyse_YC] Init OK.");
}

void Analyse_YC::End(int err_code, const std::string& err_msg /*= std::string()*/) throw(base::Exception)
{
	// 更新任务状态
	std::string task_desc;
	if ( 0 == err_code )	// 正常退出
	{
		// 更新任务状态为；"22"（分析完成）
		task_desc = "分析结束时间：" + base::SimpleTime::Now().TimeStamp();
		m_pAnaDB2->UpdateYCTaskReq(m_ycSeqID, "22", "分析完成", task_desc);
	}
	else	// 异常退出
	{
		// 更新任务状态为；"23"（分析失败）
		base::PubStr::SetFormatString(task_desc, "[ERROR] %s, ERROR_CODE: %d", err_msg.c_str(), err_code);
		m_pAnaDB2->UpdateYCTaskReq(m_ycSeqID, "23", "分析失败", task_desc);
	}

	Analyse::End(err_code, err_msg);
}

void Analyse_YC::GetExtendParaTaskInfo(std::vector<std::string>& vec_str) throw(base::Exception)
{
	if ( vec_str.size() < 4 )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "任务参数信息异常(split size:%lu), 无法拆分出业财任务流水号! [FILE:%s, LINE:%d]", vec_str.size(), __FILE__, __LINE__);
	}

	if ( !base::PubStr::Str2Int(vec_str[3], m_ycSeqID) )
	{
		throw base::Exception(ANAERR_TASKINFO_ERROR, "无效的业财任务流水号：%s [FILE:%s, LINE:%d]", vec_str[3].c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Analyse_YC] 业财稽核任务流水号：%d", m_ycSeqID);
}

void Analyse_YC::FetchTaskInfo() throw(base::Exception)
{
	Analyse::FetchTaskInfo();

	m_pLog->Output("[Analyse_YC] 获取业财稽核因子规则信息 ...");

	// 载入配置
	m_cfg.RegisterItem("TABLE", "TAB_YCRA_STATRULE");
	m_cfg.ReadConfig();
	std::string tab_rule = m_cfg.GetCfgValue("TABLE", "TAB_YCRA_STATRULE");

	m_pAnaDB2->SetTabYCStatRule(tab_rule);
	m_pAnaDB2->SelectYCStatRule(m_sKpiID, m_vecYCSInfo);
}

void Analyse_YC::AnalyseSourceData() throw(base::Exception)
{
	Analyse::AnalyseSourceData();

	// 业财稽核统计
	if ( m_vecYCSInfo.empty() )
	{
		throw base::Exception(ANAERR_ANA_YCRA_DATA_FAILED, "没有业财稽核因子规则信息! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	std::map<std::string, double> map_src;
	TransYCStatFactor(map_src);

	GenerateYCResultData(map_src);
}

void Analyse_YC::TransYCStatFactor(std::map<std::string, double>& map_factor) throw(base::Exception)
{
	double yc_val = 0.0;
	std::string yc_dim;
	std::map<std::string, double> map_f;

	const int VEC3_SIZE = m_v3HiveSrcData.size();
	for ( int i = 0; i < VEC3_SIZE; ++i )
	{
		std::vector<std::vector<std::string> >& ref_vec2 = m_v3HiveSrcData[i];

		const int VEC2_SIZE = ref_vec2.size();
		for ( int j = 0; j < VEC2_SIZE; ++j )
		{
			std::vector<std::string>& ref_vec = ref_vec2[j];

			// 因子重复！
			yc_dim = base::PubStr::TrimUpperB(ref_vec[0]);
			if ( map_f.find(yc_dim) != map_f.end() )
			{
				throw base::Exception(ANAERR_TRANS_YCFACTOR_FAILED, "重复的业财稽核维度因子: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", yc_dim.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}

			// 维度值无法转换为精度型
			if ( !base::PubStr::Str2Double(ref_vec[1], yc_val) )
			{
				throw base::Exception(ANAERR_TRANS_YCFACTOR_FAILED, "无效的业财稽核统计维度值: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_vec[1].c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}

			map_f[yc_dim] = yc_val;
		}
	}

	if ( map_f.empty() )
	{
		throw base::Exception(ANAERR_TRANS_YCFACTOR_FAILED, "没有业财稽核统计源数据! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	map_f.swap(map_factor);
	m_pLog->Output("[Analyse_YC] 统计因子转换大小：%lu", map_factor.size());

	// 释放Hive源数据
	std::vector<std::vector<std::vector<std::string> > >().swap(m_v3HiveSrcData);
}

void Analyse_YC::GenerateYCResultData(std::map<std::string, double>& map_factor) throw(base::Exception)
{
	YCStatResult yc_sr;
	std::vector<std::string> v_dat;
	std::vector<std::vector<std::string> > vec_yc_data;
	std::map<std::string, double>::iterator m_it;

	// 生成结果数据
	const int VEC_YCSI_SIZE = m_vecYCSInfo.size();
	for ( int i = 0; i < VEC_YCSI_SIZE; ++i )
	{
		YCStatInfo& ref_ycsi = m_vecYCSInfo[i];
		base::PubStr::TrimUpper(ref_ycsi.statdim_id);
		m_pLog->Output("[Analyse_YC] [STAT_ID:%s, STATDIM_ID:%s, STAT_PRIORITY:%d] 正在生成统计因子结果数据...", ref_ycsi.stat_id.c_str(), ref_ycsi.statdim_id.c_str(), ref_ycsi.stat_pri);

		if ( YCStatInfo::SP_Level_0 == ref_ycsi.stat_pri )
		{
			m_pLog->Output("[Analyse_YC] 统计因子类型：一般因子");

			yc_sr.stat_report = ref_ycsi.stat_report;
			yc_sr.stat_id     = ref_ycsi.stat_id;
			yc_sr.stat_name   = ref_ycsi.stat_name;
			yc_sr.statdim_id  = ref_ycsi.statdim_id;

			if ( (m_it = map_factor.find(ref_ycsi.statdim_id)) != map_factor.end() )
			{
				yc_sr.stat_value = m_it->second;
			}
			else
			{
				throw base::Exception(ANAERR_GENERATE_YCDATA_FAILED, "不存在的业财稽核统计维度ID: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_ycsi.statdim_id.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}

			yc_sr.Trans2Vector(v_dat);
			base::PubStr::VVectorSwapPushBack(vec_yc_data, v_dat);
		}
		else if ( YCStatInfo::SP_Level_1 == ref_ycsi.stat_pri )
		{
			m_pLog->Output("[Analyse_YC] 统计因子类型：组合因子");

			yc_sr.stat_report = ref_ycsi.stat_report;
			yc_sr.stat_id     = ref_ycsi.stat_id;
			yc_sr.stat_name   = ref_ycsi.stat_name;
			yc_sr.statdim_id  = ref_ycsi.statdim_id;
			yc_sr.stat_value  = CalcYCComplexFactor(map_factor, ref_ycsi.stat_sql);

			yc_sr.Trans2Vector(v_dat);
			base::PubStr::VVectorSwapPushBack(vec_yc_data, v_dat);
		}
		else
		{
			throw base::Exception(ANAERR_GENERATE_YCDATA_FAILED, "未知的统计因子优先级别！(KPI_ID:%s, ANA_ID:%s, STATDIM_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), ref_ycsi.statdim_id.c_str(), __FILE__, __LINE__);
		}
	}

	// 插入业财稽核结果数据
	base::PubStr::VVVectorSwapPushBack(m_v3HiveSrcData, vec_yc_data);
}

double Analyse_YC::CalcYCComplexFactor(std::map<std::string, double>& map_factor, const std::string& cmplx_factr_fmt) throw(base::Exception)
{
	m_pLog->Output("[Analyse_YC] 组合因子表达式：%s", cmplx_factr_fmt.c_str());

	// 组合因子格式：[ A1, A2, A3, ...|+, -, ... ]
	std::vector<std::string> vec_cf_1;
	base::PubStr::Str2StrVector(cmplx_factr_fmt, "|", vec_cf_1);
	if ( vec_cf_1.size() != 2 )
	{
		throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "无法识别的组合因子表达式：%s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", cmplx_factr_fmt.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	std::string yc_dims = base::PubStr::TrimUpperB(vec_cf_1[0]);
	std::string yc_oper = base::PubStr::TrimUpperB(vec_cf_1[1]);

	std::vector<std::string> vec_cf_2;
	base::PubStr::Str2StrVector(yc_dims, ",", vec_cf_1);
	base::PubStr::Str2StrVector(yc_oper, ",", vec_cf_2);

	// 至少两个统计维度；且运算符个数比维度少一个
	const int VEC_CF_SIZE = vec_cf_1.size();
	if ( VEC_CF_SIZE < 2 || (size_t)VEC_CF_SIZE != (vec_cf_2.size() + 1) )
	{
		throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "不匹配的组合因子表达式：%s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", cmplx_factr_fmt.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	// 计算结果
	std::map<std::string, double>::iterator m_it;
	double cmplx_factr_result = 0.0;
	for ( int i = 1; i < VEC_CF_SIZE; ++i )
	{
		if ( 1 == i )	// 首次
		{
			std::string& ref_dim_0 = vec_cf_1[0];
			if ( (m_it = map_factor.find(ref_dim_0)) != map_factor.end() )
			{
				cmplx_factr_result += m_it->second;
			}
			else
			{
				throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "不存在的业财稽核统计维度ID: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_dim_0.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}
		}

		std::string& ref_dim = vec_cf_1[i];
		if ( (m_it = map_factor.find(ref_dim)) != map_factor.end() )
		{
			std::string& ref_oper = vec_cf_2[i-1];
			if ( "+" == ref_oper )
			{
				cmplx_factr_result += m_it->second;
			}
			else if ( "-" == ref_oper )
			{
				cmplx_factr_result -= m_it->second;
			}
			else if ( "*" == ref_oper )
			{
				cmplx_factr_result *= m_it->second;
			}
			else if ( "/" == ref_oper )
			{
				cmplx_factr_result /= m_it->second;
			}
			else
			{
				throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "无法识别的组合因子运算符: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_oper.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
			}
		}
		else
		{
			throw base::Exception(ANAERR_CAL_YCCMPLX_FAILED, "不存在的业财稽核统计维度ID: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", ref_dim.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}
	}

	return cmplx_factr_result;
}

void Analyse_YC::AnalyseRules(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	if ( m_taskInfo.AnaRule.AnaType != AnalyseRule::ANATYPE_YC_STAT )	// 业财稽核统计类型
	{
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "不支持的业财稽核分析规则类型: %d (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.AnaRule.AnaType, m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	const std::string ANA_EXP = base::PubStr::TrimB(m_taskInfo.AnaRule.AnaExpress);
	m_pLog->Output("[Analyse_YC] 分析规则类型：业财稽核统计 (KPI_ID:%s, ANA_ID:%s)", m_sKpiID.c_str(), m_sAnaID.c_str());
	m_pLog->Output("[Analyse_YC] 分析规则表达式：%s", ANA_EXP.c_str());
	GetStatisticsHiveSQL(vec_hivesql);

	// 生成数据库[DB2]信息
	GetAnaDBInfo();
}

void Analyse_YC::AlarmJudgement() throw(base::Exception)
{
	// 业财稽核统计，暂不生成告警！
	m_pLog->Output("[Analyse_YC] 暂不生成告警！");
}

void Analyse_YC::UpdateDimValue()
{
	// 业财稽核统计，无需更新维度取值范围！
	m_pLog->Output("[Analyse_YC] 无需更新维度取值范围！");
}

