#!/usr/bin/python
#coding:utf-8

import sys, os, time, string, struct, gevent, re, traceback
import gevent.monkey
from cachetraffic_gl import tasks_repqueue
from cachetraffic_gl import sqlconpool_dict
from cachetraffic_gl import sqlconpool_sem
from cachetraffic_gl import sqlconf_dict
from cachetraffic_gl import sqlconf_sem
from gevent.queue import Queue
from gevent.lock import BoundedSemaphore
from cachetraffic_sql import Mysql
from cachetraffic_log import logger

gevent.monkey.patch_all()

class CacheTrafficRepWorker():
    def __init__(self, config, log):
        #日志对象
        self.log = log
        #配置对象
        self.config = config

    def write_historylog(self, line):
        #保存上报失败日志
        try:
            #临时写文件
            tempath = self.config.HISTORYLOGPATH + '/' + 'day_'+ str(time.time()) + '.temp'
            #正式重命名文件
            path    = self.config.HISTORYLOGPATH + '/' + 'day_'+ str(time.time()) + '.back'
            fp = open(tempath, 'w')
            lines = []
            for value in line:
                domain_data, domain, doamin_ip, domain_bandwidth = value[0:4]
                #将上报信息按行保存到文件
                nodes = domain_data+'\t'+domain+'\t'+str(doamin_ip)+'\t'+str(domain_bandwidth)+'\n'
                lines.append(nodes)
            #行写入
            fp.writelines(lines)
            fp.close()
            #写完毕，重命名
            os.rename(tempath, path)
        except:
            info = sys.exc_info()
            traceback.extract_tb(info[2])
            self.log.error('error %s %s %s', info[0], info[1], tempath)
            return 0

    def update_sql(self, line):
        
        sqlconpool_sem.acquire()
        #获取相关库的链接
        sqlclient,table = sqlconpool_dict.get("qita", None)
        #上传日表
        sql = "INSERT INTO "+ table+"(BW_DATE, BW_DOMAIN, BW_IP, BW_BANDWIDTH) VALUES(%s,%s,%s,%s)"
        if not sqlclient:
            self.log.error("reportwork sqlclient is not set %s", table)
            sqlconpool_sem.release()
            return 0
        else:
            try:
                param = line
                #插入多行数据
                ret = sqlclient.insertMany(sql, param)
                #事务提交
                sqlclient.end('commit')
            except:
                #事务回滚
                info = sys.exc_info()
                traceback.extract_tb(info[2])
                self.log.error("%s %s %s %s", info[0], info[1], sql, str(param))
                sqlconpool_sem.release()
                return 0
        sqlconpool_sem.release()
        return 1
        
    def handle_tasks(self):
        self.log.debug("upload work service start")
        #等待日表任务队列
        while 1:
            gevent.sleep(0.5)
            #如果处理队列为空， 则不处理
            if not tasks_repqueue.empty():
                #任务信息
                taskinfo    = tasks_repqueue.get()
            else:
                continue

            #上报日志信息
            ret = self.update_sql(taskinfo)
            if not ret:
                #上报失败
                self.write_historylog(taskinfo)
            else:
                self.log.debug("update sql succeed")


