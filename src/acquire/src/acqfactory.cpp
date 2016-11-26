#include "acqfactory.h"
#include "pubstr.h"
#include "acquire.h"
#include "acquire_yc.h"


AcqFactory gAcqFty;


AcqFactory::AcqFactory()
{
	g_pFactory = &gAcqFty;
}

AcqFactory::~AcqFactory()
{
}

BaseFrameApp* AcqFactory::CreateApp(const std::string& mode, const std::string& var, std::string* pError)
{
	const std::string VARIANT = base::PubStr::TrimUpperB(var);
	if ( VAR_DEBUG == VARIANT )
	{
		return CreateAcq(mode, true, pError);
	}
	else if ( VAR_RELEASE == VARIANT )
	{
		return CreateAcq(mode, false, pError);
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[ACQ_FACTORY] Create app failed - unknown variant: %s [FILE:%s, LINE:%d]", var.c_str(), __FILE__, __LINE__);
		}

		return NULL;
	}
}

void AcqFactory::DestroyApp(BaseFrameApp** ppApp)
{
	if ( ppApp != NULL && *ppApp != NULL )
	{
		delete *ppApp;
		*ppApp = NULL;
	}
}

Acquire* AcqFactory::CreateAcq(const std::string& mode, bool is_test, std::string* pError)
{
	Acquire* pAcq = NULL;

	const std::string ACQ_MODE = base::PubStr::TrimUpperB(mode);
	if ( MODE_YDJH == ACQ_MODE )
	{
		pAcq = new Acquire();
	}
	else if ( MODE_YCRA == ACQ_MODE )
	{
		pAcq = new Acquire_YC();
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[ACQ_FACTORY] Create app failed - unknown variant: %s [FILE:%s, LINE:%d]", var.c_str(), __FILE__, __LINE__);
		}

		return NULL;
	}

	if ( pAcq != NULL )
	{
		pAcq->SetTestFlag(is_test);
	}
	else
	{
		if ( pError != NULL )
		{
			base::PubStr::SetFormatString(*pError, "[ACQ_FACTORY] new Acquire failed: 无法申请到内存空间! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		}
	}

	return pAcq;
}

