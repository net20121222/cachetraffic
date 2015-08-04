#!/usr/bin/python
#coding:utf-8

import sys, os, time, string, struct, gevent, re, gzip, shutil, traceback
import gevent.monkey
from cachetraffic_gl import tasks_dayqueue
from cachetraffic_gl import tasks_daydict,sqlextensive_dict
from cachetraffic_gl import tasks_daycheckdict
from cachetraffic_gl import tasks_daysem
from cachetraffic_gl import sqlconf_dict
from cachetraffic_gl import sqlconf_sem
from gevent.queue import Queue
from gevent.lock import BoundedSemaphore
from cachetraffic_log import logger
gevent.monkey.patch_all()

class CacheTrafficDayWorker():
    def __init__(self, config, log, id):
        #日志对象
        self.log = log
        #配置对象
        self.config = config
        #跟踪ID
        self.id  = id

    def check_timestamp(self, ins):
        #检查时间戳关键域
        m = re.match('(^\d+$)', ins)
        if m:
           return 1
        else:
           return 0

    def check_domain(self, ins):
        #检查域名关键域
        m = re.match('(^[\w\d\.\:\-\_]+$)', ins)
        if m:
           return 1
        else:
           return 0

    def check_ip(self, ins):
        #检查IP关键域
        m = re.match('(\d+\.\d+\.\d+\.\d+)', ins)
        if m:
           return 1
        else:
           return 0

    def check_isp(self, ins):
        #检查ISP关键域
        m = re.match('(^[a-zA-Z-\d]+$)', ins)
        if m:
           return 1
        else:
           return 0

    def check_flag(self, ins):
        #检查动静态关键域
        m = re.match('(^[SD]$)', ins)
        if m:
           return 1
        else:
           return 0

    def check_digst(self, ins):
        #检查带宽数字类型等关键域
        m = re.match('(^\d+$)', ins)
        if m:
           return 1
        else:
           return 0

    def param_check(self, path, line):
        line.strip('\r\n')
        #拆分数据结构
        timestamp, domain, ip, isp, flag, band, num, during = re.split("[\t\s]+", line)[0:8]
        if domain.find(':'):
            domain = domain.split(":")[0]
        #检查关键域
        if not self.check_timestamp(timestamp):
            self.log.debug('timestamp format is not correct: %s', line)
            return 0
        #检查关键域
        if not self.check_domain(domain):
            self.log.debug('domain format is not correct: %s', line)
            return 0
        #检查关键域
        if not self.check_ip(ip):
            self.log.debug('ip format is not correct: %s', line)
            return 0
        '''
        #检查关键域
        if not self.check_isp(isp):
            self.log.debug('isp format is not correct: %s', line)
            return 0
        #检查关键域
        if not self.check_flag(flag):
            self.log.debug('flag format is not correct: %s', line)
            return 0
        '''
        #检查关键域
        if not self.check_digst(band):
            self.log.debug('band format is not correct: %s', line)
            return 0
        '''
        #检查关键域
        if not self.check_digst(num):
            self.log.debug('num format is not correct: %s', line)
            return 0
        #检查关键域
        if not self.check_digst(during):
            self.log.debug('during format is not correct: %s', line)
            return 0
        '''
        inputs = [timestamp, domain, ip, isp, flag, band, num, during]
        #聚合日志信息
        self.aggresion(path, *inputs)
        return 1

    def read_gz_file(self, path):
        #读取存储目录日志
        tasks_daysem.acquire()
        if os.path.exists(path):
            try:
                filename = os.path.basename(path)
                fp = gzip.open(path)
                """
                #按行读取
                firstline = fp.readline()
                #压缩类型第一行是压缩信息过滤
                splitc = firstline.split('\x00')
                leng = len(splitc)
                """
                #剩下来的数据读入缓存
                cache = fp.read()
                for line in cache.splitlines():
                    #行解析
                    self.param_check(filename, line)
                fp.close()
            except:
                info = sys.exc_info()
                traceback.extract_tb(info[2])
                self.log.error('open file or check file is not correct %s %s %s', info[0], info[1], path)
                tasks_daysem.release()
                return 0
        else:
            self.log.debug('file is not exit %s', path)
            tasks_daysem.release()
            return 0
        tasks_daysem.release()
        return 1

    def backuplog(self,path):
        #日志备份
        filename = os.path.basename(path)
        if not filename:
            self.log.error("basename is null %s", path)
            #错误日志
            self.backuperrlog(path)
            return 0
        else:
            #根据IP，UUID， 时间戳组合文件名
            macheip, uuid, timestamp, other = re.split('_', filename)
            #取时间戳
            timearray = time.localtime(float(timestamp))
            arrays = time.strftime("%Y-%m-%d %H:%M:%S", timearray)
            year, month, day, other = re.split('[-\s]', arrays)
            config_dir = os.getcwd()
            tempath = macheip + '/'+ year + '/' + month + '/' + day + '/'
            #拼接当前路径目录的配置文件
            backup_path = os.path.join(self.config.BACKLOGPATH, tempath)
            filepath= os.path.join(backup_path, filename)
            if os.path.exists(filepath):
				os.remove(filepath)
            #取路径名
            dir= os.path.dirname(backup_path)
            #创建目录
            if not os.path.isdir(dir):
                try:
                    os.makedirs(dir)
                except:
                    info = sys.exc_info()
                    traceback.extract_tb(info[2])
                    self.log.error("can not makedir %s %s", info[0], info[1])
                    self.backuperrlog(path)
                    return 0
            try:
                #转移日志
                shutil.move(path, backup_path)
            except:
                info = sys.exc_info()
                traceback.extract_tb(info[2])
                self.log.error("can not makedir %s %s", info[0], info[1])
                self.backuperrlog(path)
                return 0
            return 1


    def backuperrlog(self,path):
        #转移错误日志
        filename = os.path.basename(path)
        if not filename:
            self.log.error("basename is null %s", path)
            return 0
        #取路径名
        dir= os.path.dirname(self.config.BACKERRORLOGPATH)
        filepath= os.path.join(self.config.BACKERRORLOGPATH, filename)
        if os.path.exists(filepath):
            os.remove(filepath)
        #创建目录
        if not os.path.isdir(dir):
            try:
                os.mkdir(dir)
            except:
                info = sys.exc_info()
                traceback.extract_tb(info[2])
                self.log.error("can not makdir %s %s", info[0], info[1])
        try:
            #转移日志
            shutil.move(path, self.config.BACKERRORLOGPATH)
        except:
            info = sys.exc_info()
            traceback.extract_tb(info[2])
            self.log.error("can not move %s %s", info[0], info[1])

    def aggresion(self, path, *kw):
        #聚合日志
        #取时间戳
        timearray = time.localtime(float(kw[0]))
        arrays = time.strftime("%Y-%m-%d %H:%M:%S", timearray)
        year, month, day, other = re.split('[-\s]', arrays)

        date = year+month+day
        domain = kw[1]
        cache_ip = str(kw[2])
        bandwidth = int(kw[5])
        insert = {}
        bextensive = False
        sqlconf_sem.acquire()
        #查询字典， 存在对象则合并数据
        oth = sqlconf_dict.get(domain, None)
        if oth:
            sqlconf_sem.release()
            return
        else:
            # 泛域名分析
            for extensivekey in sqlextensive_dict.keys():
                if re.match(str(extensivekey), domain):
                    bextensive = True
                    break
            if bextensive:
                sqlconf_sem.release()
                return
        sqlconf_sem.release()
        dir_valuekey = {"domain_ip":cache_ip,"domain_time":date}
        #合并同类对象
        dir_value = tasks_daydict.get(str(domain), None)
        if not dir_value:
            insert  = {str(domain):{str(dir_valuekey):bandwidth}}
            #插入新数据
            tasks_daydict.update(insert)
        else:
            dir_valuevalue = tasks_daydict[domain].get(str(dir_valuekey),None)
            if not dir_valuevalue:
                insert  = {str(dir_valuekey):bandwidth}            
            else:
                #更新数据， 带宽聚合
                bandwidth += dir_valuevalue
                insert  = {str(dir_valuekey):bandwidth}
                tasks_daydict[domain].update(insert)
        


    def handle_tasks(self):
        #等待任务队列
        while 1:
            #如果处理队列为空， 则不处理
            if not tasks_dayqueue.empty():
                taskinfo = tasks_dayqueue.get()
            else:
                gevent.sleep(1)
                continue

            self.log.debug("dayqueue taskinfo: %s", str(taskinfo))
            #处理日志
            if self.read_gz_file(taskinfo):
                #备份日志
                self.backuplog(taskinfo)
            else:
                #转移错误日志
                self.backuperrlog(taskinfo)


