#include <iostream>
#include <string.h>
#include <unistd.h>
#include "pubstr.h"
#include "log.h"
#include "config.h"
#include "tasksche.h"


int main(int argc, char* argv[])
{
	// DAEMON_FLAG: 守护进程标志位（'1'-有效）
	// LOG_ID     ：日志 ID
	// CFG_FILE   ：配置文件
	if ( argc != 4 )
	{
		std::cerr << "[usage]" << argv[0] << " DAEMON_FLAG LOG_ID CFG_FILE" << std::endl;
		return -1;
	}

	// Daemon process ?
	bool is_daemon = (strcmp(argv[1], "1") == 0);
	if ( is_daemon )
	{
		pid_t f_pid = fork();
		if ( f_pid < 0 )	// fork error !
		{
			std::cerr << "Process fork error: " << f_pid << std::endl;
			return -1;
		}
		else if ( f_pid > 0 )	// Parent process end !
		{
			std::cout << "[DAEMON] Parent process [pid=" << getpid() << "] end" << std::endl;
			std::cout << "[DAEMON] Child process [pid=" << f_pid << "] start" << std::endl;
			return 0;
		}
	}

	// 日志配置
	long long log_id = 0;
	if ( !base::PubStr::Str2LLong(argv[2], log_id) )
	{
		std::cerr << "[ERROR] Unknown log ID: '" << argv[2] << "'" << std::endl;
		return -1;
	}
	if ( !base::Log::SetLogID(log_id) )
	{
		std::cerr << "[ERROR] Invalid log ID: " << log_id << std::endl;
		return -1;
	}

	// 日志文件前缀
	base::Log::SetLogFilePrefix("TaskSche");

	base::AutoLogger aLog;
	base::Log* pLog = aLog.Get();

	try
	{
		// 读取配置
		base::Config cfg;
		cfg.SetCfgFile(argv[3]);
		cfg.RegisterItem("SYS", "LOG_PATH");
		cfg.ReadConfig();

		pLog->SetPath(cfg.GetCfgValue("SYS", "LOG_PATH"));
		pLog->Init();
		pLog->Output("%s: PID=[%d]", (is_daemon?"守护进程":"一般进程"), getpid());

		TaskSche ts(cfg);

		// 输出版本号
		std::cout << ts.Version() << std::endl;
		pLog->Output(ts.Version());

		ts.Do();
	}
	catch ( base::Exception& ex )
	{
		std::cerr << "[ERROR] " << ex.What() << ", ERROR_CODE: " << ex.ErrorCode() << std::endl;
		pLog->Output("[ERROR] %s, ERROR_CODE: %d", ex.What().c_str(), ex.ErrorCode());
		return -2;
	}
	catch ( ... )
	{
		std::cerr << "[ERROR] Unknown error! [FILE:" << __FILE__ << ", LINE:" << __LINE__ << "]" << std::endl;
		pLog->Output("[ERROR] Unknown error! [FILE:%s, LINE:%d]", __FILE__, __LINE__);
		return -3;
	}

	std::cout << argv[0] << " quit !" << std::endl;
	pLog->Output("%s quit !", argv[0]);
	return 0;
}

