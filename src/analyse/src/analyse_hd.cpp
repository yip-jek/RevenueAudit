#include "analyse_hd.h"
#include "log.h"
#include "pubstr.h"
#include "canadb2.h"
#include "sqltranslator.h"

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
	m_pLog->Output("[Analyse_HD] 分析规则表达式：%s", m_taskInfo.AnaRule.AnaExpress.c_str());

	// 生成数据库[DB2]信息
	GetAnaDBInfo();

	// 生成数据删除的时间(段)
	GenerateDeleteTime();

	// 从分析表达式中获得取数逻辑
	GetExpressHiveSQL(vec_hivesql);

	// 生成执行SQL队列
	GetExecuteSQL();
}

void Analyse_HD::GetExpressHiveSQL(std::vector<std::string>& vec_hivesql) throw(base::Exception)
{
	// HIVE SQL 语句就在分析表达式中
	std::string hive_sql = base::PubStr::TrimB(m_taskInfo.AnaRule.AnaExpress);
	// 去除末尾可能存在的分号(';')
	int sql_size = hive_sql.size();
	while ( sql_size > 0 )
	{
		if ( ';' == hive_sql[sql_size-1] )
		{
			hive_sql.erase(sql_size-1);
			sql_size = hive_sql.size();
		}
		else
		{
			break;
		}
	}

	if ( hive_sql.empty() )
	{
		throw base::Exception(ANAERR_GET_EXP_HIVESQL_FAILED, "[HDJH] NO effective hive sql in analyse express! (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	ExchangeSQLMark(hive_sql);
	m_pLog->Output("[Analyse_HD] HIVE SQL: %s", hive_sql.c_str());

	if ( !GetSequenceInHiveSQL(hive_sql) )
	{
		throw base::Exception(ANAERR_GET_EXP_HIVESQL_FAILED, "[HDJH] Illegal hive sql: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", hive_sql.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}

	std::vector<std::string>().swap(vec_hivesql);
	vec_hivesql.push_back(hive_sql);
}

bool Analyse_HD::GetSequenceInHiveSQL(std::string& hive_sql)
{
}

void Analyse_HD::GenerateDeleteTime() throw(base::Exception)
{
	// 没有设置时间标志
	// 删除时间(段)在分析规则的备注2字段
	const std::string DEL_TIME_FMT = base::PubStr::TrimB(m_taskInfo.AnaRule.Remark_2);
	if ( DEL_TIME_FMT.empty() )
	{
		return;
	}

	std::vector<std::string> vec_str;
	base::PubStr::Str2StrVector(DEL_TIME_FMT, ",", vec_str);

	const int VEC_SIZE = vec_str.size();
	if ( 1 == VEC_SIZE )
	{
		std::string& ref_str = vec_str[0];
		ExchangeSQLMark(ref_str);

		if ( !base::PubStr::Str2Int(ref_str, m_begintime) )
		{
			throw base::Exception(ANAERR_GENE_DELTIME_FAILED, "[HDJH] 无法识别的时间标志: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", DEL_TIME_FMT.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
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
			throw base::Exception(ANAERR_GENE_DELTIME_FAILED, "[HDJH] 无法识别的时间标志: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", DEL_TIME_FMT.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}

		if ( !base::PubStr::Str2Int(ref_str_e, m_endtime) )
		{
			throw base::Exception(ANAERR_GENE_DELTIME_FAILED, "[HDJH] 无法识别的时间标志: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", DEL_TIME_FMT.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
		}
	}
	else
	{
		throw base::Exception(ANAERR_GENE_DELTIME_FAILED, "[HDJH] 无法识别的时间标志: %s (KPI_ID:%s, ANA_ID:%s) [FILE:%s, LINE:%d]", DEL_TIME_FMT.c_str(), m_sKpiID.c_str(), m_sAnaID.c_str(), __FILE__, __LINE__);
	}
}

void Analyse_HD::GetExecuteSQL()
{
	// 格式: [insert sql];[insert sql];...
	// 执行SQL语句在分析规则的备注3字段
	base::PubStr::Str2StrVector(m_taskInfo.AnaRule.Remark_3, ";", m_vecExecSql);

	// 去除空SQL语句，并进行标志转换
	int vec_size = m_vecExecSql.size();
	for ( int i = 0; i < vec_size; ++i )
	{
		std::string& ref_str = m_vecExecSql[i];
		if ( ref_str.empty() )
		{
			m_vecExecSql.erase(m_vecExecSql.begin()+i);
			--i;
			--vec_size;
		}
		else
		{
			ExchangeSQLMark(ref_str);
		}
	}
}

void Analyse_HD::GenerateTableNameByType() throw(base::Exception)
{
	const base::PubTime::DATE_TYPE DT = m_dbinfo.GetEtlDateType();
	const std::string ETL_DAY         = m_dbinfo.GetEtlDay();

	ReleaseSQLTranslator();
	m_pSQLTranslator = new SQLTranslator(DT, ETL_DAY);
	if ( NULL == m_pSQLTranslator )
	{
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "new SQLTranslator failed: 无法申请到内存空间! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	// 最终目标表在分析规则的备注1字段
	const std::string TAB_TARGET = base::PubStr::TrimB(m_taskInfo.AnaRule.Remark_1);
	if ( TAB_TARGET.empty() )
	{
		throw base::Exception(ANAERR_GENERATE_TAB_FAILED, "目标表名没有设置！(分析规则表的备注1字段) [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	m_dbinfo.target_table = TAB_TARGET;
	m_pLog->Output("[Analyse_HD] 最终目标表名：%s", TAB_TARGET.c_str());

	// 置空：非报表统计没有备份表
	m_dbinfo.backup_table.clear();
}

void Analyse_HD::DataSupplement()
{
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

