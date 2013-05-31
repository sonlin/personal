#include "connect_pool.h"

using common::mysql::DbConfig;
using common::mysql::ConnectPool;

int main() {
  DbConfig config;
  config.dbHost = "127.0.0.1";
  config.dbPort = 3306;
  config.userName = "zone";
  config.passWd = "";
  config.dbName = "zone_channel_admin";
  config.maxConnections = 16;


  ConnectPool pool(&config);
  sql::Connection * con = pool.GetConnection();
  std::auto_ptr<sql::PreparedStatement> prep_stmt(con->prepareStatement("select * from ChannelAdmin"));
  std::auto_ptr< sql::ResultSet > res(prep_stmt->executeQuery());

  int row = 0;
  while(res->next()) {
    std::cout<<"row:"<<row<<", getRow:"<<res->getRow()<<std::endl;
    std::cout<<"userAccount:"<<res->getString("userAccount")<<std::endl;
    row++;
  }


}
