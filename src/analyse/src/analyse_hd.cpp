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

	// HIVE SQL 语句就在分析表达式中
	// 格式一: [hive sql] (暂只支持单个HIVE SQL语句)
	// 格式二: [时间标志];[hive sql] (指定数据删除的时间)
	// 格式三: [开始时间标志, 结束时间标志];[hive sql] (指定数据删除的时间段)
	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(hive_sql, ";", vec_str);

	std::vector<std::string> vec_sql;
	tmp_size = vec_str.size();

	if ( 0 == tmp_size )
	{
		throw base::Exception(ANAERR_GET_EXP_HIVESQL_FAILED, "[HDJH] NO effective hive sql in analyse express! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
	else if ( 1 == tmp_size || 2 == tmp_size )
	{
		if ( 2 == tmp_size )
		{
			GenerateDeleteTime(vec_str[0]);
		}

		hive_sql = vec_str[tmp_size-1];
		ExchangeSQLMark(hive_sql);

		vec_sql.push_back(hive_sql);
		m_pLog->Output("[Analyse_HD] 分析规则表达式（HIVE SQL）：%s", hive_sql.c_str());
	}
	else
	{
		throw base::Exception(ANAERR_GET_EXP_HIVESQL_FAILED, "[HDJH] Not support multiple hive sql(s) in analyse express: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", hive_sql.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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
	// 删除旧的数据
	RemoveOldResult(m_taskInfo.ResultType);

	// 组织入库 SQL 语句
	std::string str_sql = "insert into " + m_dbinfo.target_table + "(";

	std::string str_holder;
	int t_size = m_taskInfo.vecKpiDimCol.size();
	for ( int i = 0; i < t_size; ++i )
	{
		KpiColumn& ref_dim = m_taskInfo.vecKpiDimCol[i];

		if ( i > 0 )
		{
			str_sql    += (", " + ref_dim.DBName);
			str_holder += ", ?";
		}
		else
		{
			str_sql    += ref_dim.DBName;
			str_holder += "?";
		}
	}

	t_size = m_taskInfo.vecKpiValCol.size();
	for ( int j = 0; j < t_size; ++j )
	{
		KpiColumn& ref_val = m_taskInfo.vecKpiValCol[j];
		str_sql    += (", " + ref_val.DBName);
		str_holder += ", ?";
	}

	str_sql += ") values(" + str_holder + ")";

	// 入库话单稽核结果数据
	t_size = m_v3HiveSrcData.size();
	for ( int k = 0; k < t_size; ++k )
	{
		m_pLog->Output("[Analyse_HD] 准备入库第 %d 组结果数据 ...", (k+1));
		m_pAnaDB2->ResultDataInsert(str_sql, "", false, m_v3HiveSrcData[k]);
	}

	m_pLog->Output("[Analyse_HD] 结果数据存储完毕!");
}

void Analyse_HD::RemoveOldResult(const AnaTaskInfo::ResultTableType& result_tabtype) throw(base::Exception)
{
	// 是否带时间戳
	// 只有带时间戳才可以按采集时间删除结果数据
	if ( m_dbinfo.time_stamp )
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

