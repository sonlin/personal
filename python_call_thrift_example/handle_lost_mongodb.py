#!/usr/bin/python
#-*- coding: UTF-8 -*-
import MySQLdb
import socket
import os
import datetime
import time
import sys
file_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(file_path, 'gen-py'))
import fetch.ttypes
import fetch.FetchService
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import logging.config
logging.config.fileConfig(os.path.join(file_path, 'logging.conf'))

import config_test
import sendmail

logger = None
_client = None
db_connection = None
db_cursor = None
today = datetime.datetime.today()

def mysql_connect(config):
    """
    连接数据库
    """
    conn = MySQLdb.connect(host=config['host'],
                         user=config['user'],
                         port=config['port'],
                         passwd=config['passwd'],
                         db=config['db'],
                         charset=config['charset'])
    conn.autocommit(False)
    return conn


def real_thrift_server_connect(host, port):
    """
    连接thrift服务器
    """
    transportSocket = TSocket.TSocket(host, port)
    transport = TTransport.TFramedTransport(transportSocket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = fetch.FetchService.Client(protocol)
    transport.open()
    return client

def connect_server(host, port):
    """
    连接thrift服务器，尝试3次
    """
    global _client
    retries = 3
    for retry in range(retries, 0, -1):
        try:
            if not _client:
                _client = real_thrift_server_connect(host, port)
            else:
                return
        except:
            logger.exception('can not connect thrift server!')
            if retry == 1:
                raise
            time.sleep(1)
            _client = None


def init(config):
    global db_connection,db_connection,db_cursor,logger
    logger = logging.getLogger("thriftServer")
    db_connection =  mysql_connect(config)
    db_cursor = db_connection.cursor()


def mainpage_new_game_process(new_games):
    logger.info('start mainpage_new_game_process ...')
    refresh_sql = "Delete FROM zhyy_new_game_info;"
    db_cursor.execute(refresh_sql)

    index_id = 0
    for game in new_games:
        add_sql = """
        INSERT INTO 
            zhyy_new_game_info(id,gameId,serverId,dateTime) 
        VALUES(%(id)s,%(gameId)s,%(serverId)s,%(dateTime)s)
        """ 
        params = {'id':index_id,'gameId':game.gameId,
                'serverId':game.serverId,'dateTime':game.date}
        db_cursor.execute(add_sql, params)
        db_connection.commit()
        index_id += 1
    logger.info('finish mainpage_new_game_process')



def find_uncomplete_task():
    sql="select * from FetchRecord where status <> 'COMPLETE'"
    db_cursor.execute(sql);
    items = []
    for item in db_cursor.fetchall():
        items.append(item)
    return items

def main():
    global _client
    init(config_test.db_config)
    try:
        connect_server(config_test.thrift_server['ip'],
                config_test.thrift_server['port'])
        result = _client.getUrlBySrcUrl('http://a.9game.cn/game/downs_10_2.htm')
        print "result is : ",result
        uncomplete_task = find_uncomplete_task()
        print uncomplete_task
    except Exception, e:
        sendmail.mail(config_test.sendmail['sender'],
                config_test.sendmail['receivers'],
                'test',
                str(e))

if __name__ == '__main__':
    main()
