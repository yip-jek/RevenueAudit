#include "anadbinfo.h"
#include "simpletime.h"

AnaDBInfo::AnaDBInfo()
:date_type(base::PubTime::DT_UNKNOWN)
{
}

AnaDBInfo::~AnaDBInfo()
{
}

bool AnaDBInfo::GenerateDayTime(const std::string& etl_time)
{
	// 生成当前时间（天）和采集时间（天）
	tf_nowday.day_time = base::SimpleTime::Now().DayTime8();
	return base::PubTime::DateApartFromNow(etl_time, date_type, tf_etlday.day_time);
}

bool AnaDBInfo::SetEtlDayIndex(int index)
{
	if ( index >= 0 )	// 有效
	{
		tf_etlday.index = index;
		return true;
	}
	else	// 无效
	{
		tf_etlday.index = TimeField::TF_INVALID_INDEX;
		return false;
	}
}

bool AnaDBInfo::SetNowDayIndex(int index)
{
	if ( index >= 0 )	// 有效
	{
		tf_nowday.index = index;
		return true;
	}
	else	// 无效
	{
		tf_nowday.index = TimeField::TF_INVALID_INDEX;
		return false;
	}
}

base::PubTime::DATE_TYPE AnaDBInfo::GetEtlDateType() const
{
	return date_type;
}

int AnaDBInfo::GetEtlDayIndex() const
{
	return tf_etlday.index;
}

int AnaDBInfo::GetNowDayIndex() const
{
	return tf_nowday.index;
}

std::string AnaDBInfo::GetEtlDay() const
{
	return tf_etlday.day_time;
}

std::string AnaDBInfo::GetNowDay() const
{
	return tf_nowday.day_time;
}

void AnaDBInfo::SetAnaFields(const std::vector<AnaField>& v_fields)
{
	vec_fields.assign(v_fields.begin(), v_fields.end());
}

int AnaDBInfo::GetFieldSize() const
{
	return vec_fields.size();
}

AnaField AnaDBInfo::GetAnaField(int index) const
{
	if ( index >= 0 && index < (int)vec_fields.size() )
	{
		return vec_fields[index];
	}

	AnaField af;
	return af;
}

std::string AnaDBInfo::GetEtlDayFieldName() const
{
	if ( tf_etlday.index != TimeField::TF_INVALID_INDEX && tf_etlday.index < (int)vec_fields.size() )
	{
		return vec_fields[tf_etlday.index].field_name;
	}

	return std::string();
}

