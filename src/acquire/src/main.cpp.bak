#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <poll.h>

#include <iostream>

#include <boost/lexical_cast.hpp>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include <transport/TTransport.h>
#include <protocol/TProtocol.h>

#include "ThriftHive.h"

#include "log.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace Apache::Hadoop::Hive;


int main(int argc, char** argv)
{
	base::Log::SetCCMID(99988);

	base::AutoLogger alog;
	base::Log* pLog = alog.Get();
	pLog->SetPath(".");
	pLog->Init();

	pLog->Output("BOOST_VERSION: %d", BOOST_VERSION);
	pLog->Output("BOOST_LIB_VERSION: %s", BOOST_LIB_VERSION);

  if (argc < 4) {
    std::cerr << "Invalid arguments!\n" << "Usage: DemoClient host port" << std::endl;

	pLog->Output("Invalid arguments!");
	pLog->Output("Usage: DemoClient host port");
    return -1;
  }
  bool isFramed = false;
  boost::shared_ptr<TTransport> socket(new TSocket(argv[1], boost::lexical_cast<int>(argv[2])));
  boost::shared_ptr<TTransport> transport;

  if (isFramed) {
    transport.reset(new TFramedTransport(socket));
  } else {
    transport.reset(new TBufferedTransport(socket));
  }
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

  	ThriftHiveClient client(protocol);
  	
  	try {
    transport->open();
    std::cout << "Scanner begin" << std::endl;
	pLog->Output("Scanner begin");
    
    std::string table(argv[3]);
    std::string qry1("select * from ");
    std::string qry = qry1 + table ;   
    std::vector<std::string> result;
    std::cout << "Query:" << qry << std::endl;
	pLog->Output("Query: %s", qry.c_str());
    client.execute(qry);    
    std::cout << "Query return ok." << std::endl;
	pLog->Output("Query return ok.");

    //client.fetchAll(result);
    client.fetchN(result, 10000);
    {
    	for(int i = 0; i < result.size(); ++i)
    	{
    		std::cout << result[i]<<std::endl;
    	}
    	result.clear();
    	client.fetchN(result, 10000);
    }while(result.size() > 0)
    std::cout << "get return ok." << std::endl;   	
	pLog->Output("get return ok.");
    
    
    /*for(int i = 0; i < result.size(); ++i)
    {
    	std::cout << result[i]<<std::endl;
    }*/

    std::cout << "Scanner finished" << std::endl;
	pLog->Output("Scanner finished");
    transport->close();
    } catch(const TApplicationException&tx) {
    	std::cerr << "ERROR: "<< tx.what() << std::endl;
		pLog->Output("ERROR: %s", tx.what());
    }catch (const TException &tx) {
    	std::cerr << "ERROR: " << tx.what() << std::endl;
		pLog->Output("ERROR: %s", tx.what());
    }

    return 0;
	  /*
	  
	  TSocket *socket = new TSocket("192.168.35.199", 10000);
    socket->setConnTimeout(3000);
    TTransport *transport = socket;
    transport->open();
    TProtocol protocol = new TBinaryProtocol(transport);
    */
    
    /*
    DemoService.Client client = new DemoService.Client.Factory().getClient(protocol);
    int result = client.demoMethod(param1, param2, param3);
    System.out.println("result: " + result);
    transport.close();
    */


    //TTransport *transport = new TSocket("192.168.35.199", 10000);
    //TProtocol *protocol = new TBinaryProtocol(*transport);
    //ThriftHive.Client client = new ThriftHive.Client(protocol);

    //transport->Open();
    //client.execute("add file /data/home/script/ad_resolve2.py;");
    //client->execute("select * from ljh_emp");
    //Console.WriteLine("the result is:");

    
    /*var items = client->fetchAll();
    foreach (var item in items)
    {
        Console.WriteLine(item);
    }*/
    //transport.Close();

    //Console.ReadLine();
}

