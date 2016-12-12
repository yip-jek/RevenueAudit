#include "analyse_hd.h"
#include "log.h"
#include "pubstr.h"
#include "canadb2.h"

Analyse_HD::Analyse_HD()
:m_begintime(0)
,m_endtime(0)
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

	// 生成数据库[DB2]信息
	GetAnaDBInfo();

	// 从分析表达式中获得取数逻辑
	GetExpressHiveSQL(vec_hivesql);
}

void Analyse_HD::GetExpressHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	// 去除结尾的可能存在的分号（';'）
	std::string hive_sql = base::PubStr::TrimB(m_taskInfo.AnaRule.AnaExpress);
	int tmp_size = hive_sql.size();
	while ( tmp_size > 0 )
	{
		if ( ';' == hive_sql[tmp_size-1] )
		{
			hive_sql.erase(tmp_size-1);
			tmp_size = hive_sql.size();
		}
		else
		{
			break;
		}
	}
	m_pLog->Output("[Analyse_HD] 分析规则表达式：%s", hive_sql.c_str());

	// HIVE SQL 语句就在分析表达式中
	// 格式一: [hive sql];[insert sql];[insert sql];...
	// 格式二: [时间标志];[hive sql];[insert sql];[insert sql];... (指定数据删除的时间)
	// 格式三: [开始时间标志, 结束时间标志];[hive sql];[insert sql];[insert sql];... (指定数据删除的时间段)
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(hive_sql, ";", vec_str);

	// 去除空SQL语句，并进行标志转换
	tmp_size = vec_str.size();
	for ( int i = 0; i < tmp_size; ++i )
	{
		std::string& ref_str = vec_str[i];
		if ( ref_str.empty() )
		{
			vec_str.erase(vec_str.begin()+i);
			--i;
			--tmp_size;
		}
		else	// 标志转换
		{
			ExchangeSQLMark(ref_str);
		}
	}

	std::vector<std::string> vec_sql;
	tmp_size = vec_str.size();
	if ( 0 == tmp_size )
	{
		throw base::Exception(ANAERR_GET_EXP_HIVESQL_FAILED, "[HDJH] NO effective hive sql in analyse express! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
	else if ( 1 == tmp_size )
	{
		std::string sel_sql = base::PubStr::UpperB(vec_str[0]);
		if ( sel_sql.substr(0, 7) != "SELECT " )
		{
			throw base::Exception(ANAERR_GET_EXP_HIVESQL_FAILED, "[HDJH] Not support hive sql in analyse express: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", hive_sql.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}

		vec_sql.push_back(vec_str[0]);
	}
	else if ( 2 == tmp_size )
	{
		std::string sel_sql = base::PubStr::UpperB(vec_str[0]);
		if ( sel_sql.substr(0, 7) == "SELECT " )
		{
			vec_sql.push_back(vec_str[0]);
			m_vecExecSql.push_back(vec_str[1]);
		}
		else
		{
			GenerateDeleteTime(vec_str[0])

			vec_sql.push_back(vec_str[1]);
		}
	}
	else	// 3 或以上
	{
		int i = 0;
		std::string sel_sql = base::PubStr::TrimUpperB(vec_str[0]);
		if ( sel_sql.substr(0, 7) == "SELECT " )
		{
			vec_sql.push_back(vec_str[0]);

			i = 1;
		}
		else
		{
			GenerateDeleteTime(vec_str[0]);

			vec_sql.push_back(vec_str[1]);

			i = 2;
		}

		while ( i < tmp_size )
		{
			m_vecExecSql.push_back(vec_str[i++]);
		}
	}

	vec_sql.swap(vec_hivesql);
}

void Analyse_HD::GenerateDeleteTime(const std::string time_fmt) throw(base::Exception)
{
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(time_fmt, ",", vec_str);

	const int VEC_SIZE = vec_str.size();
	if ( 1 == VEC_SIZE )
	{
		std::string& ref_str = vec_str[0];
		ExchangeSQLMark(ref_str);

		if ( !base::PubStr::Str2Int(ref_str, m_begintime) )
		{
			throw base::Exception(ANAERR_GENE_DELTIME_FAILED, "[HDJH] 无法识别的时间标志: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", time_fmt.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}
	}
	else if ( 2 == VEC_SIZE )
	{
		std::string& ref_str_b = vec_str[0];
		std::string& ref_str_e = vec_str[1];
		ExchangeSQLMark(ref_str_b);
		ExchangeSQLMark(ref_str_e);

		if ( !base::PubStr::Str2Int(ref_str_b, m_begintime) )
		{
			throw base::Exception(ANAERR_GENE_DELTIME_FAILED, "[HDJH] 无法识别的时间标志: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", time_fmt.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}

		if ( !base::PubStr::Str2Int(ref_str_e, m_endtime) )
		{
			throw base::Exception(ANAERR_GENE_DELTIME_FAILED, "[HDJH] 无法识别的时间标志: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", time_fmt.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}
	}
	else
	{
		throw base::Exception(ANAERR_GENE_DELTIME_FAILED, "[HDJH] 无法识别的时间标志: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", time_fmt.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
}

void Analyse_HD::StoreResult() throw(base::Exception)
{
	Analyse::StoreResult();

	std::vector<std::vector<std::string> > tmp_vec2_data;
	const int VEC_SIZE = m_vecExecSql.size();
	for ( int i = 0; i < VEC_SIZE; ++i )
	{
		m_pAnaDB2->ResultDataInsert(m_vecExecSql[i], tmp_vec2_data);
	}
}

void Analyse_HD::RemoveOldResult(const AnaTaskInfo::ResultTableType& result_tabtype) throw(base::Exception)
{
	// 是否带时间戳
	// 只有带时间戳才可以按采集时间删除结果数据
	if ( m_dbinfo.IsEtlDayValid() )
	{
		// 结果表类型是否为天表？
		if ( AnaTaskInfo::TABTYPE_DAY == result_tabtype )
		{
			m_pLog->Output("[Analyse_HD] 清空天表数据 ...");
			m_pAnaDB2->DeleteResultData(m_dbinfo, true);
		}
		else
		{
			m_pLog->Output("[Analyse_HD] 删除已存在的结果数据 ...");

			if ( m_begintime != m_endtime )		// 删除时间段
			{
				m_pAnaDB2->DeleteTimeResultData(m_dbinfo, m_begintime, m_endtime);
			}
			else
			{
				if ( m_begintime != 0 )		// 删除指定时间
				{
					m_pAnaDB2->DeleteTimeResultData(m_dbinfo, m_begintime, 0);
				}
				else
				{
					m_pAnaDB2->DeleteResultData(m_dbinfo, false);
				}
			}
		}
	}
	else
	{
		m_pLog->Output("[Analyse_HD] 无法按时间区分结果数据，因此不进行删除操作！");
	}
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

