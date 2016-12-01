#include "analyse_hd.h"
#include "log.h"
#include "pubstr.h"

Analyse_HD::Analyse_HD()
{
	m_sType = "话单稽核";
}

Analyse_HD::~Analyse_HD()
{
}

std::string Analyse_HD::GetLogFilePrefix()
{
	return std::string("Analyse_HD");
}

void Analyse_HD::AnalyseRules(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	if ( m_taskInfo.AnaRule.AnaType != AnalyseRule::ANATYPE_HDJH_STAT )		// 话单稽核统计类型
	{
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "不支持的话单稽核分析规则类型: %d (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_taskInfo.AnaRule.AnaType, m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	m_pLog->Output("[Analyse_HD] 分析规则类型：话单稽核统计 (KPI_ID:%s, ANA_ID:%s)", m_sKpiID.c_str(), m_sAnaID.c_str());

	// 去除结尾的可能存在的分号（';'）
	std::string hive_sql = base::PubStr::TrimB(m_taskInfo.AnaRule.AnaExpress);
	const int HIVE_SQL_SIZE = hive_sql.size();
	if ( HIVE_SQL_SIZE > 0 && ';' == hive_sql[HIVE_SQL_SIZE-1] )
	{
		hive_sql.erase(HIVE_SQL_SIZE-1);
	}

	if ( hive_sql.empty() )
	{
		throw base::Exception(ANAERR_ANA_RULE_FAILED, "[HDJH] NO effective hive sql in analyse express (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
	m_pLog->Output("[Analyse_HD] 分析规则表达式（HIVE SQL）：%s", hive_sql.c_str());

	std::vector<std::string> vec_sql;
	vec_sql.push_back(hive_sql);
	vec_sql.swap(vec_hivesql);

	// 生成数据库[DB2]信息
	GetAnaDBInfo();
}

void Analyse_HD::AlarmJudgement() throw(base::Exception)
{
	// 业财稽核统计，暂不生成告警！
	m_pLog->Output("[Analyse_HD] 暂不生成告警！");
}

void Analyse_HD::UpdateDimValue()
{
	// 业财稽核统计，无需更新维度取值范围！
	m_pLog->Output("[Analyse_HD] 无需更新维度取值范围！");
}

