#pragma once

namespace base
{
class Log;
}

typedef void sgl_fun(int);

class GSignal
{
public:
	static bool Init(base::Log* pLog);
	static bool IsRunning();

private:
	static sgl_fun* SetSignal(int sig_no, sgl_fun* f);
	static void SglFuncQuit(int sig);

private:
	static base::Log* s_pLog;
	static bool       s_bQuit;
};

