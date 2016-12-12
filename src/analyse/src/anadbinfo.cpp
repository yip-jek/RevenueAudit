#include "anadbinfo.h"
#include "simpletime.h"

AnaDBInfo::AnaDBInfo()
:date_type(base::PubTime::DT_UNKNOWN)
{
}

AnaDBInfo::~AnaDBInfo()
{
}

bool AnaDBInfo::GenerateEtlDay(const std::string& etl_time, int index)
{
	if ( base::PubTime::DateApartFromNow(etl_time, date_type, tf_etlday.str_time) )
	{
		tf_etlday.valid = true;
		tf_etlday.index = index;
		return true;
	}
	else
	{
		date_type       = base::PubTime::DT_UNKNOWN;
		tf_etlday.valid = false;
		tf_etlday.index = TimeField::TF_INVALID_INDEX;
		tf_etlday.str_time.clear();
		return false;
	}
}

void AnaDBInfo::GenerateNowDay(bool is_valid, int index)
{
	if ( is_valid )
	{
		tf_nowday.valid    = true;
		tf_nowday.index    = index;
		tf_nowday.str_time = base::SimpleTime::Now().DayTime8();
	}
	else
	{
		tf_nowday.valid    = false;
		tf_nowday.index    = TimeField::TF_INVALID_INDEX;
		tf_nowday.str_time.clear();
	}
}

base::PubTime::DATE_TYPE AnaDBInfo::GetEtlDateType() const
{
	return date_type;
}

bool AnaDBInfo::IsEtlDayValid() const
{
	return tf_etlday.valid;
}

bool AnaDBInfo::IsNowDayValid() const
{
	return tf_nowday.valid;
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
	return tf_etlday.str_time;
}

std::string AnaDBInfo::GetNowDay() const
{
	return tf_nowday.str_time;
}

void AnaDBInfo::SetAnaFields(std::vector<AnaField>& v_fields)
{
	vec_fields.swap(v_fields);
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
	if ( tf_etlday.valid )
	{
		return vec_fields[tf_etlday.index].field_name;
	}

	return std::string();
}

