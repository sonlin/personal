#ifndef __CONNECT_POOL_h__
#define __CONNECT_POOL_h__
#include<mysql_driver.h> 
#include<mysql_connection.h>  
#include<cppconn/driver.h>  
#include<cppconn/exception.h>  
#include<cppconn/connection.h>  
#include<cppconn/resultset.h>  
#include<cppconn/prepared_statement.h>  
#include<cppconn/statement.h>  

namespace common { namespace mysql {

  using std::string;
  using sql::Connection;
  using sql::Driver;
  using sql::ConnectOptionsMap;

  struct DbConfig {
    string dbHost;
    int dbPort;
    string userName;
    string passWd;
    string dbName;
    int maxConnections;
  };

  class ConnectPool {
    public:
      explicit ConnectPool(DbConfig * config);
      ~ConnectPool();
      Connection * GetConnection();
      void ReleaseConnection(Connection *con);
    private:
      Connection * CreateConnection();
      void InitConPool(int size);
      void DestoryConnection(Connection *con);
      void DestoryPool();
    private:
      Driver *driver_;
      int curSize_;
      int maxSize_;
      bool reConnect;
      int timeOut;
      pthread_mutex_t lock_;
      DbConfig *dbConfig_;
      ConnectOptionsMap connectionProperties_;
      std::list<Connection *> conList_;
  };
}}

#endif
