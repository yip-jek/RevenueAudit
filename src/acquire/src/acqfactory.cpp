#include "acqfactory.h"
#include "pubstr.h"
#include "acquire_yd.h"
#include "acquire_yc.h"
#include "acquire_hd.h"


AcqFactory gAcqFty;

const char* const AcqFactory::S_MODE_YDJH = "ETL_YDJH";				// 一点稽核
const char* const AcqFactory::S_MODE_YCRA = "ETL_YCRA";				// 业财稽核
const char* const AcqFactory::S_MODE_HDJH = "ETL_HDJH";				// 话单稽核

AcqFactory::AcqFactory()
{
	g_pFactory = &gAcqFty;
}

AcqFactory::~AcqFactory()
{
}

base::BaseFrameApp* AcqFactory::CreateApp(const std::string& mode, std::string* pError)
{
	base::BaseFrameApp* pApp = NULL;

	const std::string ACQ_MODE = base::PubStr::TrimUpperB(mode);
	if ( S_MODE_YDJH == ACQ_MODE )
	{
		pApp = new Acquire_YD();
	}
	else if ( S_MODE_YCRA == ACQ_MODE )
	{
		pApp = new Acquire_YC();
	}
	else if ( S_MODE_HDJH == ACQ_MODE )
	{
		pApp = new Acquire_HD();
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[ACQ_FACTORY] Create app failed - unsupport mode: %s\nOnly support mode: %s %s %s [FILE:%s, LINE:%d]", mode.c_str(), S_MODE_YDJH, S_MODE_YCRA, S_MODE_HDJH, __FILE__, __LINE__);
		}

		return NULL;
	}

	if ( NULL == pApp && pError != NULL )
	{
		base::PubStr::SetFormatString(*pError, "[ACQ_FACTORY] Operate new failed: 无法申请到内存空间! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	return pApp;
}

void AcqFactory::DestroyApp(base::BaseFrameApp** ppApp)
{
	if ( ppApp != NULL && *ppApp != NULL )
	{
		delete *ppApp;
		*ppApp = NULL;
	}
}

