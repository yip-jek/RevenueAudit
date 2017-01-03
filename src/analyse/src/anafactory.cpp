#include "anafactory.h"
#include "pubstr.h"
#include "analyse_yd.h"
#include "analyse_yc.h"
#include "analyse_hd.h"


AnaFactory gAnaFty;

const char* const AnaFactory::S_MODE_YDJH = "ANA_YDJH";				// 一点稽核
const char* const AnaFactory::S_MODE_YCRA = "ANA_YCRA";				// 业财稽核
const char* const AnaFactory::S_MODE_HDJH = "ANA_HDJH";				// 话单稽核

AnaFactory::AnaFactory()
{
	g_pFactory = &gAnaFty;
}

AnaFactory::~AnaFactory()
{
}

base::BaseFrameApp* AnaFactory::CreateApp(const std::string& mode, std::string* pError)
{
	base::BaseFrameApp* pApp = NULL;

	const std::string ANA_MODE = base::PubStr::TrimUpperB(mode);
	if ( S_MODE_YDJH == ANA_MODE )
	{
		pApp = new Analyse_YD();
	}
	else if ( S_MODE_YCRA == ANA_MODE )
	{
		pApp = new Analyse_YC();
	}
	else if ( S_MODE_HDJH == ANA_MODE )
	{
		pApp = new Analyse_HD();
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[ANA_FACTORY] Create app failed - unsupport mode: %s\nOnly support mode: %s %s %s [FILE:%s, LINE:%d]", mode.c_str(), S_MODE_YDJH, S_MODE_YCRA, S_MODE_HDJH, __FILE__, __LINE__);
		}

		return NULL;
	}

	if ( NULL == pApp && pError != NULL )
	{
		base::PubStr::SetFormatString(*pError, "[ANA_FACTORY] Operate new failed: 无法申请到内存空间! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
	}

	return pApp;
}

void AnaFactory::DestroyApp(base::BaseFrameApp** ppApp)
{
	if ( ppApp != NULL && *ppApp != NULL )
	{
		delete *ppApp;
		*ppApp = NULL;
	}
}

