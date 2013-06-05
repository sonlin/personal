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
from fetch.ttypes import *
from FetchService import FetchService
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

db_connection = mysql_connect(config_test.db_config)
db_cursor = db_connection.cursor()

def zhyy_server_connect(host, port):
    """
    连接珠海页游thrift服务器
    """
    transportSocket = TSocket.TSocket(host, port)
    transport = TTransport.TFramedTransport(transportSocket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = ZhuHaiYeYou.Client(protocol)
    transport.open()
    return client

def connect_zhyy_server():
    global _client
    """
    连接珠海页游thrift服务器,尝试3次
    """
    retries = 3
    for retry in range(retries, 0, -1):
        try:
            if not _client:
                _client = zhyy_server_connect(
                        config_test.zhuhai_server['ip'],
                        config_test.zhuhai_server['port'])
        except:
            logger.exception('can not connect to zhuihai thrift server')
            if retry == 1:
                raise
            time.sleep(1)
            _client = None

def strict_mode():
    strict_sql = "set @@sql_mode=STRICT_TRANS_TABLES;"
    db_cursor.execute(strict_sql)


def mainpage_new_server_process(new_servers):
    """
    获取新开服信息。行为：清除原有数据，然后插入新数
    """
    logger.info('start mainpage_new_server_process ...')
    refresh_sql = "Delete FROM zhyy_new_server_info;"
    db_cursor.execute(refresh_sql)

    index_id = 0
    for server in new_servers:
        add_sql = """
        INSERT INTO 
            zhyy_new_server_info(id,gameId,serverId,dateTime) 
        VALUES(%(id)s,%(gameId)s,%(serverId)s,%(dateTime)s)
        """ 
        params = {'id':index_id,'gameId':server.gameId,
                'serverId':server.serverId,'dateTime':server.date}
        db_cursor.execute(add_sql, params)
        index_id += 1
    logger.info('finish mainpage_new_server_process')

def mainpage_new_game_process(new_games):
    """
    获取新游戏信息。行为：清除原有数据，然后插入新数
    """
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
        index_id += 1
    logger.info('finish mainpage_new_game_process')

def mainpage_tab_process(tab_info):
    """
    获取首页下方tab标签信息。行为：清除原有数据，然后插入新数
    """
    logger.info('start mainpage_tab_process ...')
    refresh_sql = "Delete FROM zhyy_tab_info;"
    db_cursor.execute(refresh_sql)

    index_id = 0
    for item in tables:
        for gameId in item.gameIds: 
            add_sql = """
            INSERT INTO 
                zhyy_tab_info(id,title,gameId) 
            VALUES(%(id)s,%(title)s,%(gameId)s)
            """ 
            params = {'id':index_id,'title':item.title,'gameId':gameId }
            db_cursor.execute(add_sql, params)
            index_id += 1
    logger.info('finish mainpage_tab_process')

def mainpage_process():
    """
    处理首页的所有广告信息（在同一个事务里处理）
    """
    logger.info('start mainpage_process ...')
    
    new_servers = _client.listNewServerInfo()
    logger.info('finish fetch new server info')
    new_games = _client.listNewGameInfo()
    logger.info('finish fetch new game info')
    tab_info = _client.listTabInfo()
    logger.info('finish fetch tab info')
    
    strict_mode()
    mainpage_new_server_process(new_servers)
    mainpage_new_game_process(new_games)
    mainpage_tab_process(tab_info)
    
    # 提交事务
    db_connection.commit()
    logger.info('finish mainpage_process')

def baseinfo_games_process(games):
    """
    获取游戏信息。行为：保留并取出原有数据与新数据比较，
    考虑到上下架的情况，所以按实际情况更新
    """
    logger.info('start baseinfo_games_process ...')
    game_ids = set()
    for item in games:
        game_ids.add(item.gameId)
    get_old_data_sql = """
    SELECT gameId FROM zhyy_game_info WHERE isDeleted = 0;
    """
    db_cursor.execute(get_old_data_sql)
    old_game_ids = set()
    for item in db_cursor.fetchall():
        old_game_ids.add(item[0])

    # 找到下架的id，并标志isDeleted为1
    down_games = old_game_ids - game_ids
    if len(down_games) > 0:
        logger.info('下架游戏id列表:%s',str(down_games))
        for down_game_id in down_games:
            handle_down_games_sql = """
            update zhyy_game_info set isDeleted = 1 where gameId = %(gameId)s;
            """ 
            params = {'gameId':down_game_id}
            db_cursor.execute(handle_down_games_sql. params)

    #INSERT INTO .....  ON DUPLICATE KEY UPDATE .....
    for game in games:
        add_sql = """
        INSERT INTO 
            zhyy_game_info(gameId,gameName,gameType,gameIconUrl,
            gameImageUrl,gameGrade,gameIntroduction,defaultServerId,isDeleted)
        VALUES(%(gameId)s,%(gameName)s,%(gameType)s,%(gameIconUrl)s,
            %(gameImageUrl)s,%(gameGrade)s,%(gameIntroduction)s,
            %(defaultServerId)s,%(isDeleted)s)
        ON DUPLICATE KEY UPDATE
            gameId=%(gameId2)s,gameName=%(gameName2)s,gameType=%(gameType2)s,
            gameIconUrl=%(gameIconUrl2)s,gameImageUrl=%(gameImageUrl2)s,
            gameGrade=%(gameGrade2)s,gameIntroduction=%(gameIntroduction2)s,
            defaultServerId=%(defaultServerId2)s,isDeleted=%(isDeleted2)s;
        """ 
        params = {'gameId':game.gameId, 'gameName':game.gameName,
               'gameType':game.gameType,'gameIconUrl':game.gameIconUrl,
               'gameImageUrl':game.gameImageUrl,'gameGrade':game.gameGrade,
               'gameIntroduction':game.gameIntroduction,
               'defaultServerId':game.defaultServerId,'isDeleted':0,
               ######
               'gameId2':game.gameId, 'gameName2':game.gameName,
               'gameType2':game.gameType,'gameIconUrl2':game.gameIconUrl,
               'gameImageUrl2':game.gameImageUrl,'gameGrade2':game.gameGrade,
               'gameIntroduction2':game.gameIntroduction,
               'defaultServerId2':game.defaultServerId,'isDeleted2':0}
        db_cursor.execute(add_sql, params)
    logger.info('finish baseinfo_games_process')


def baseinfo_servers_process(servers):
    """
    获取服务器信息。行为：保留并取出原有数据与新数据比较，
    考虑到上下架的情况，所以按实际情况更新
    """
    logger.info('start baseinfo_servers_process ...')
    server_ids = set()
    for item in servers:
        temp_list=[]
        temp_list.append(item.gameId)
        temp_list.append(item.serverId)
        server_ids.add(tuple(temp_list))

    get_old_data_sql = """
    SELECT gameId,serverId FROM zhyy_game_server_info WHERE isDeleted = 0;
    """
    db_cursor.execute(get_old_data_sql)
    old_server_ids = set()
    for item in db_cursor.fetchall():
        old_server_ids.add(item)

    # 找到下架的id，并标志isDeleted为1
    down_servers = old_server_ids - server_ids
    if len(down_servers) > 0:
        logger.info('下架区服id列表:%s',str(down_servers))
        for item in down_servers:
            handle_down_servers_sql = """
            update zhyy_game_server_info set isDeleted = 1
                where gameId=%(gameId)s and serverId = %(serverId)s;
            """ 
            params = {'gameId':item[0],'serverId':item[1]}
            db_cursor.execute(handle_down_servers_sql, params)

    #INSERT INTO .....  ON DUPLICATE KEY UPDATE .....
    for server in servers:
        add_sql = """
        INSERT INTO 
            zhyy_game_server_info(gameId,serverId,onlineGiftId,serverName,
            serverDescription,yyUrl,isDeleted)
        VALUES(%(gameId)s,%(serverId)s,%(onlineGiftId)s,%(serverName)s,
            %(serverDescription)s,%(yyUrl)s,%(isDeleted)s)
        ON DUPLICATE KEY UPDATE
            gameId=%(gameId2)s,serverId=%(serverId2)s,
            onlineGiftId=%(onlineGiftId2)s,serverName=%(serverName2)s,
            serverDescription=%(serverDescription2)s,yyUrl=%(yyUrl2)s,
            isDeleted=%(isDeleted2)s;
        """ 
        params = {'gameId':server.gameId,'serverId':server.serverId,
               'onlineGiftId':server.onlineGiftId,
               'serverName':server.serverName,
               'serverDescription':server.serverDescription,
               'yyUrl':server.yyUrl,'isDeleted':0,
               ######
               'gameId2':server.gameId,'serverId2':server.serverId,
               'onlineGiftId2':server.onlineGiftId,
               'serverName2':server.serverName,
               'serverDescription2':server.serverDescription,
               'yyUrl2':server.yyUrl,'isDeleted2':0}
        db_cursor.execute(add_sql, params)
    logger.info('finish baseinfo_servers_process')


def baseinfo_gifts_process(gifts):
    """
    获取礼包信息。行为：保留并取出原有数据与新数据比较，
    考虑到上下架的情况，所以按实际情况更新
    """
    logger.info('start baseinfo_gifts_process ...')
    gift_ids = set()
    for item in gifts:
        temp_list=[]
        temp_list.append(item.gameId)
        temp_list.append(item.serverId)
        temp_list.append(item.giftId)
        gift_ids.add(tuple(temp_list))

    get_old_data_sql = """
    SELECT gameId,serverId,giftId FROM zhyy_gift_info WHERE isDeleted = 0;
    """
    db_cursor.execute(get_old_data_sql)
    old_gift_ids = set()
    for item in db_cursor.fetchall():
        old_gift_ids.add(item)

    # 找到下架的id，并标志isDeleted为1
    down_gifts = old_gift_ids - gift_ids
    if len(down_gifts) > 0:
        logger.info('下架礼包id列表:%s',str(down_gifts))
        for item in down_gifts:
            handle_down_gifts_sql = """
            update zhyy_gift_info set isDeleted = 1
                where gameId=%(gameId)s and serverId = %(serverId)s
                    and giftId = %(giftId)s;
            """ 
            params = {'gameId':item[0],'serverId':item[1],'giftId':item[2]}
            db_cursor.execute(handle_down_gifts_sql)

    #INSERT INTO .....  ON DUPLICATE KEY UPDATE .....
    for gift in gifts:
        add_sql = """
        INSERT INTO 
            zhyy_gift_info(giftId,gameId,serverId,giftDescription,
            activationIntro,beginTime,endTime,isDeleted)
        VALUES(%(giftId)s,%(gameId)s,%(serverId)s,%(giftDescription)s,
            %(activationIntro)s,%(beginTime)s,%(endTime)s,%(isDeleted)s)
        ON DUPLICATE KEY UPDATE
            giftId=%(giftId2)s,gameId=%(gameId2)s,serverId=%(serverId2)s,
            giftDescription=%(giftDescription2)s,activationIntro=%(activationIntro2)s,
            beginTime=%(beginTime2)s,endTime=%(endTime2)s,
            isDeleted=%(isDeleted2)s;
        """ 
        params = {'giftId':gift.giftId,'gameId':gift.gameId,
               'serverId':gift.serverId,
               'giftDescription':gift.giftDescription,
               'activationIntro':gift.activationIntro,
               'beginTime':gift.beginTime,
               'endTime':gift.endTime, 'isDeleted':0,
               ######
               'giftId2':gift.giftId,'gameId2':gift.gameId,
               'serverId2':gift.serverId,
               'giftDescription2':gift.giftDescription,
               'activationIntro2':gift.activationIntro,
               'beginTime2':gift.beginTime,
               'endTime2':gift.endTime, 'isDeleted':0}
        db_cursor.execute(add_sql, params)
    logger.info('finish baseinfo_gifts_process')

def baseinfo_process():
    """
    处理所有的游戏、区服、礼包信息（在同一个事务里处理）
    """
    logger.info('start baseinfo_process ...')
    
    games = _client.listGameInfo()
    logger.info('finish listGameInfo')
    servers = _client.listServerInfo()
    logger.info('finish listServerInfo')
    gifts = _client.listGiftInfo()
    logger.info('finish listGiftInfo')
    
    strict_mode()
    baseinfo_games_process(games)
    baseinfo_servers_process(servers)
    baseinfo_gifts_process(gifts)
    # 提交事务
    db_connection.commit()
    logger.info('finish baseinfo_process')


def fetch_cards_process(giftId, count):
    """
    获取卡码。行为：保留所有数据，然后插入新数据
    """
    logger.info('   fetch cards info begin ...')
    measure = 1000 
    logger.info('   need to fetch %s cards for giftId %s,fetch %s cards each time',
            str(count), str(giftId), str(measure))

    if count%measure == 0:
        num = int(count/measure)
    else:
        num = int(count/measure) + 1

    for i in range(0,num):
        cards_list=[]
        addedCount = 0
        
        if i != num:
            logger.info('   fetch %s cards for the %s time', str(measure), str(i))
            card_codes = _client.fetchCards(giftId,measure)
        else:
            logger.info('   fetch %s cards for the %s time', str(count - i*measure), str(i))
            card_codes = _client.fetchCards(giftId,count - i*measure)
        for code in card_codes:
            cards_list.append("(%d,%d)" % (giftId, code))
        
        if not card_codes:
            continue
        
        addedCount += len(cards_list)
        
        strict_mode()
        
        add_sql = """
        INSERT INTO zhyy_card_code(giftId,cardCode) VALUES %s
        """ % (','.join(cards_list))
        db_cursor.execute(add_sql)

        cnt_sql = """
        INSERT INTO zhyy_card_count(giftId,totalCount,usedCount) VALUES(%s,%s,%s)
        ON DUPLICATE KEY UPDATE totalCount = totalCount + value(totalCount)
        """
        db_cursor.execute(cnt_sql,(giftId, addedCount, 0))
        
        logger.info("add %d to total count for giftId=%d", addedCount, giftId)
        
        db_connection.commit()

    logger.info('   fetch cards info end ...')

def cards_process():
    """
    卡码数量少于阈值就取一个阈值数量的card,此处定义阈值为10000。
    """
    logger.info('check cards info begin ...')

    FETCH_COUNT = 10000
    MIN_HOLD_COUNT = 1000
    
    get_giftIds_sql = """
    select i.giftId, ifnull(c.totalCount,0), ifnull(c.usedCount,0) from zhyy_gift_info i left join zhyy_card_count c on c.giftId = i.giftId
    where i.isDeleted = 0 and ifnull(c.totalCount,0) - ifnull(c.usedCount,0) < %(remainCount)s
    """
    params = {remainCount: MIN_HOLD_COUNT}
    db_cursor.execute(get_giftIds_sql, params)

    gift_list = []
    for item in db_cursor.fetchall():
        gift_list.append(item)

    for item in gift_list:
        giftId, totalCount, usedCount = item
        fetchCount = FETCH_COUNT - abs(totalCount - usedCount)
        logger.info('cards quantity belong to giftId %s is less than %s,then fetch %s cards', giftId, MIN_HOLD_COUNT, fetchCount)
        fetch_cards_process(giftId, fetchCount)

    logger.info('check cards info end ...')
    

def process():
    global logger
    global _client
    baseinfo_process()
    mainpage_process()
    cards_process()


def main():
    global logger
    global _client
    logger = logging.getLogger("zhuhaiyeyouClient")
    try:
        connect_zhyy_server()
        process()
    except Exception, e:
        sendmail.mail(config_test.sendmail['sender'],
                config_test.sendmail['receivers'],
                '珠海页游数据同步脚本故障',
                str(e))
        
    print "ok"


if __name__ == '__main__':
    main()

