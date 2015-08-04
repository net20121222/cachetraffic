#!/usr/bin/python
#coding:utf-8

import sys, os, time, string, struct, gevent, re, gzip, shutil, traceback
import gevent.monkey
from cachetraffic_gl import tasks_historyqueue
from cachetraffic_gl import sqlconpool_dict
from cachetraffic_gl import sqlconpool_sem
from gevent.queue import Queue
from gevent.lock import BoundedSemaphore
from cachetraffic_sql import Mysql
from cachetraffic_log import logger
gevent.monkey.patch_all()

class CacheTrafficHistoryWorker():
    def __init__(self, config, log):
        #日志对象
        self.log = log
        #配置对象
        self.config = config

    def param_check(self, line, dicts):
        line.strip('\r\n')
        #拆分数据结构
        domain_data, domain, doamin_ip, domain_bandwidth = re.split("[\t\s]+", line)[0:4]
        dir_valuekey = {'domain_ip':doamin_ip, 'domain_time':domain_data}
        dir_value = dicts.get(str(domain), None)
        if not dir_value:
            insert = {str(domain):{str(dir_valuekey):int(domain_bandwidth)}}
            #插入字典对象
            dicts.update(insert)
        else:
            dir_valuevalue = dicts[domain].get(str(dir_valuekey),None)
            if not dir_valuevalue:
                insert  = {str(dir_valuekey):int(domain_bandwidth)}            
            else:
                #更新数据， 带宽聚合
                bandwidth = dir_valuevalue + int(domain_bandwidth)
                insert  = {str(dir_valuekey):bandwidth}
                dicts[domain].update(insert)

        return 1

    def read_file(self, path):
        #读取存储目录日志
        if os.path.exists(path):
            try:
                history_dicts = {}
                fp = open(path, 'r')
                #数据读入缓存
                cache = fp.read()
                for line in cache.splitlines():
                    #行解析
                    self.param_check(line, history_dicts)
                fp.close()
                #上报内容
                ret = self.aggresion(history_dicts)
                if ret:
                    #上报成功将文件删除
                    os.remove(path)
            except:
                info = sys.exc_info()
                traceback.extract_tb(info[2])
                self.log.error('history error %s %s %s', info[0], info[1], path)
                return 0
        else:
            self.log.debug('history file is not exit %s', path)
            return 0
        return 1

    def update_sql(self, line):
        sqlconpool_sem.acquire()
        #获取相关库的链接
        sqlclient,table = sqlconpool_dict.get("qita", None)
        #上传日表
        sql = "INSERT INTO "+ table+"(BW_DATE, BW_DOMAIN, BW_IP, BW_BANDWIDTH) VALUES(%s,%s,%s,%s)"
        if not sqlclient:
            self.log.error("history work sqlclient is not set %s", table)
            sqlconpool_sem.release()
            return 0
        else:
            try:
                param = line
                #插入多行
                ret = sqlclient.insertMany(sql, param)
                #事务提交
                sqlclient.end('commit')
            except:
                #发生错误， 事务回滚
                info = sys.exc_info()
                traceback.extract_tb(info[2])
                self.log.error("%s %s %s %s", info[0], info[1], sql, str(param))
                sqlconpool_sem.release()
                return 0
        sqlconpool_sem.release()
        return 1

    def aggresion(self, dicts):
        info = []
        for strDomain,strvalue in dicts.items():
            for k,strDomainTime in strvalue.items():
                strCacheIP = eval(k)
                list_lineinfo = [strCacheIP["domain_time"],strDomain,strCacheIP["domain_ip"],strDomainTime]
                info.append(list_lineinfo)
        if info:
            ret = self.update_sql(info)
            return ret
        return 0

    def handle_tasks(self):
        self.log.debug("upload history service start")
        #等待任务队列
        while 1:
            #如果处理队列为空， 则不处理
            if not tasks_historyqueue.empty():
                taskinfo = tasks_historyqueue.get()
            else:
                gevent.sleep(5)
                continue

            self.log.debug("history queue taskinfo: %s", str(taskinfo))
            #处理历史日志
            self.read_file(taskinfo)


