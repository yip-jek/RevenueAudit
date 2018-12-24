#include <iostream>
#include <string.h>
#include <unistd.h>
#include "pubstr.h"
#include "log.h"
#include "config.h"
#include "task.h"
#include "taskfactory.h"


int main(int argc, char* argv[])
{
	// DAEMON_FLAG: 守护进程标志位（'1'-有效）
	// LOG_ID     ：日志 ID
	// MODE       : 模式
	// CFG_FILE   ：配置文件
	if ( argc != 5 )
	{
		std::cerr << "[usage]" << argv[0] << " DAEMON_FLAG LOG_ID MODE CFG_FILE" << std::endl;
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

	base::LogStrategy log_strategy;
	log_strategy.log_id = log_id;

	base::AutoLogger aLog;
	base::Log* pLog = aLog.Get();

	try
	{
		// 读取配置
		base::Config cfg;
		cfg.SetCfgFile(argv[4]);
		cfg.RegisterItem("LOG", "PATH");
		cfg.RegisterItem("LOG", "MAX_LINE");
		cfg.RegisterItem("LOG", "MAX_SIZE");
		cfg.RegisterItem("LOG", "INTERVAL");
		cfg.ReadConfig();

		log_strategy.log_path = cfg.GetCfgValue("LOG", "PATH");
		log_strategy.max_line = cfg.GetCfgLongVal("LOG", "MAX_LINE");
		log_strategy.max_size = cfg.GetCfgLongVal("LOG", "MAX_SIZE");
		log_strategy.interval = cfg.GetCfgValue("LOG", "INTERVAL");

		TaskFactory t_factory(cfg);
		std::string str_tmp;
		Task* pTask = t_factory.Create(argv[3], &str_tmp);
		if ( NULL == pTask )
		{
			std::cerr << "[ERROR] Create task failed: " << str_tmp << std::endl;
			return -1;
		}

		base::PubStr::SetFormatString(str_tmp, "%s: PID=[%d]", (is_daemon?"守护进程":"一般进程"), getpid());
		log_strategy.log_headers.push_back(str_tmp);
		log_strategy.log_headers.push_back(pTask->Version());
		log_strategy.log_prefix = pTask->LogPrefix();

		pLog->Init(log_strategy);

		std::cout << pTask->Version() << std::endl;
		pTask->Run();
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

