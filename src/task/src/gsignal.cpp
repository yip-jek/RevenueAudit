#include "gsignal.h"
#include <iostream>
#include <signal.h>
#include "log.h"

base::Log* GSignal::s_pLog = NULL;
bool GSignal::s_bQuit = false;

bool GSignal::Init(base::Log* pLog)
{
	if ( NULL == pLog )
	{
		return false;
	}
	s_pLog = pLog;

	if ( SetSignal(SIGTERM, SglFuncQuit) == SIG_ERR )
	{
		std::cout << "Signal SIGTERM error!" << std::endl;
		s_pLog->Output("Signal SIGTERM error!");
		return false;
	}

	if ( SetSignal(SIGINT, SglFuncQuit) == SIG_ERR )
	{
		std::cout << "Signal SIGINT error!" << std::endl;
		s_pLog->Output("Signal SIGINT error!");
		return false;
	}

	if ( SetSignal(SIGQUIT, SglFuncQuit) == SIG_ERR )
	{
		std::cout << "Signal SIGQUIT error!" << std::endl;
		s_pLog->Output("Signal SIGQUIT error!");
		return false;
	}

	std::cout << "[INIT] Set signal OK." << std::endl;
	s_pLog->Output("[INIT] Set signal OK.");
	return true;
}

bool GSignal::IsRunning()
{
	return !s_bQuit;
}

sgl_fun* GSignal::SetSignal(int sig_no, sgl_fun* f)
{
	struct sigaction sig_ac;
	sig_ac.sa_handler = f;
	sigemptyset(&sig_ac.sa_mask);
	sig_ac.sa_flags = 0;
	if ( SIGALRM == sig_no )
	{
#ifdef SA_INTERRUPT
		sig_ac.sa_flags |= SA_INTERRUPT;
#endif 
	}
	else
	{
#ifdef SA_RESTART
		sig_ac.sa_flags |= SA_RESTART;
#endif
	}

	struct sigaction n_sig_ac;
	if ( sigaction(sig_no, &sig_ac, &n_sig_ac) < 0 )
	{
		return SIG_ERR;
	}

	return n_sig_ac.sa_handler;
}

void GSignal::SglFuncQuit(int sig)
{
	s_bQuit = true;

	if ( SIGTERM == sig )
	{
		std::cout << "Signal SIGTERM received! Ready to quit ..." << std::endl;
		s_pLog->Output("Signal SIGTERM received! Ready to quit ...");
	}
	else if ( SIGINT == sig )
	{
		std::cout << "Signal SIGINT received! Ready to quit ..." << std::endl;
		s_pLog->Output("Signal SIGINT received! Ready to quit ...");
	}
	else if ( SIGQUIT == sig )
	{
		std::cout << "Signal SIGQUIT received! Ready to quit ..." << std::endl;
		s_pLog->Output("Signal SIGQUIT received! Ready to quit ...");
	}
	else
	{
		std::cout << "Unknown signal (" << sig << ") received! Ready to quit ..." << std::endl;
		s_pLog->Output("Unknown signal (%d) received! Ready to quit ...", sig);
	}
}

