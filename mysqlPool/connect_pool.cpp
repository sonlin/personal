#include "connect_pool.h"

namespace common { namespace mysql {
  ConnectPool::ConnectPool(DbConfig *config) {
    this->dbConfig_ = config;
    curSize_ = 0;
    maxSize_ = dbConfig_->maxConnections;
    driver_ = NULL;
    connectionProperties_["hostName"] = dbConfig_->dbHost;
    connectionProperties_["port"] = dbConfig_->dbPort;
    connectionProperties_["userName"] = dbConfig_->userName;
    connectionProperties_["password"] = dbConfig_->passWd;
    connectionProperties_["schema"] = dbConfig_->dbName;
    connectionProperties_["OPT_CHARSET_NAME"] = string("utf8");
    pthread_mutex_init(&lock_, NULL);
    try {
      driver_ = sql::mysql::get_driver_instance();
    } catch (sql::SQLException &e) {
      std::cout<< "# ERR: " << e.what() << " (MySQL error code: " 
        << e.getErrorCode() << ", SQLState: " << e.getSQLState() 
        << " )" << std::endl;
      return; 
    } catch (std::runtime_error &e) {
      std::cout << "# ERR: " << e.what() << std::endl;
      return;
    }
    InitConPool(maxSize_/2);
  }

  void ConnectPool::InitConPool(int size) {
    Connection *con;
    pthread_mutex_lock(&lock_);
    for(int i = 0; i < size; ++i)
    {
      con = CreateConnection();
      if(con != NULL) {
        conList_.push_back(con);
        curSize_++;
      }
    }
    pthread_mutex_unlock(&lock_);
  }

  ConnectPool::~ConnectPool() {
    DestoryPool();
    pthread_mutex_destroy(&lock_);
  }

  Connection *ConnectPool::CreateConnection() {
    Connection * con;
    try {
      con = driver_->connect(connectionProperties_);
    } catch (sql::SQLException &e) {
      std::cout<< "# ERR: " << e.what() << " (MySQL error code: " 
        << e.getErrorCode() << ", SQLState: " << e.getSQLState() 
        << " )" << std::endl;
      return NULL;
    } catch (std::runtime_error &e) {
      std::cout << "# ERR: " << e.what() << std::endl;
      return NULL;
    }
    return con;
  }

  void ConnectPool::DestoryConnection(Connection *con) {
    try {
      if(con != NULL) {
        con->close();
        delete con;
      }
    } catch (sql::SQLException &e) {
      std::cout<< "# ERR: " << e.what() << " (MySQL error code: " 
        << e.getErrorCode() << ", SQLState: " << e.getSQLState() 
        << " )" << std::endl;
    } catch (std::runtime_error &e) {
      std::cout << "# ERR: " << e.what() << std::endl;
    }
  }

  Connection * ConnectPool::GetConnection() {
    pthread_mutex_lock(&lock_);
    Connection *con = NULL;
    if(conList_.size() > 0) {
      con = conList_.front();
      conList_.pop_front();
      if(con->isClosed()) {
        delete con;
        con = CreateConnection();
        if(con == NULL) {
          curSize_--;
        }
      }
    } else {
      if(curSize_ < maxSize_) {
        con = CreateConnection();
        if(con != NULL) {
          curSize_++;
        }
      }
    }
    pthread_mutex_unlock(&lock_);
    return con;
  }

  void ConnectPool::ReleaseConnection(Connection *con) {
    pthread_mutex_lock(&lock_);
    conList_.push_back(con);
    pthread_mutex_unlock(&lock_);
  }
  void ConnectPool::DestoryPool() {
    pthread_mutex_lock(&lock_);
    std::list<Connection *>::iterator it = conList_.begin();
    for(; it != conList_.end(); ++it) {
      DestoryConnection(*it);
    }
    curSize_ = 0;
    conList_.clear();
    pthread_mutex_unlock(&lock_);
  }
}}
