#!/usr/bin/python
#coding:utf-8

import sys, os, time, string, struct, gevent, re, traceback, signal, socket
import gevent.monkey
from cachetraffic_gl import tasks_dayqueue
from cachetraffic_gl import tasks_daydict
from cachetraffic_gl import tasks_daysem
from cachetraffic_gl import tasks_historyqueue
from cachetraffic_gl import tasks_repqueue
from cachetraffic_gl import sqlconf_dict
from cachetraffic_gl import sqlconf_sem
from cachetraffic_gl import sqlconpool_dict,sqlextensive_dict
from cachetraffic_gl import sqlconpool_sem
from gevent.queue import Queue
from gevent.lock import BoundedSemaphore
from suds.client import Client
from suds.xsd.doctor import ImportDoctor,Import
from cachetraffic_serverconfig import CacheTrafficServerConfig
from cachetraffic_sql import Mysql
from cachetraffic_daywork import CacheTrafficDayWorker 
from cachetraffic_historywork import CacheTrafficHistoryWorker 
from cachetraffic_reportwork import CacheTrafficRepWorker
from cachetraffic_log import logger

gevent.monkey.patch_all()


if __name__ == '__main__':

    def loadconfig():
        #载入客户端配置
        #配置路径
        config_dir  = "/usr/local/cachetraffic/"
        #拼接当前路径目录的配置文件
        config_path = os.path.join(config_dir, "etc/cachetraffic_server.ini")
        #取路径名
        dir= os.path.dirname(config_path)
        #判断路径是否存在， 否则创建
        if not os.path.isdir(dir):
            os.mkdir(dir)

        #创建配置对象
        config = CacheTrafficServerConfig(config_path)
        #返回配置对象
        return config

    def loginit():
        #配置路径
        config_dir  = "/usr/local/cachetraffic/"
        #拼接当前路径目录的配置文件
        config_path = os.path.join(config_dir, "log/cachetraffic_server.log")
        #拼接当前路径目录的配置文件
        dir= os.path.dirname(config_path)
        #判断路径是否存在， 否则创建
        if not os.path.isdir(dir):
            os.mkdir(dir)
        #创建日志对象
        log=logger(config_path)
        #返回日志对象
        return log

    def sigint_handler(signum, frame):
        #信号处理，全局的信号灯
        global is_sigint_up
        #收到中断信号， 信号灯亮起
        is_sigint_up = True
        log.debug("catched interrupt signal!")

    def update_sqlconfig():
        #初始化客户id和域名id
        while True:
            #数据库接口取客户信息域名信息域名类型
            sql = "SELECT DNI_ID,DNI_DNAME,DNI_CUSID,DNI_TYPE from domainnameinfotest"
            try:
                results = sqlconfclient.getAll(sql)
                sqlconfclient.end()
            except:
                info = sys.exc_info()
                traceback.extract_tb(info[2])
                log.error("update_wsdlconfig %s %s %s", info[0], info[1], sql)
                gevent.sleep(60*10)
                continue
            if results > 0:
                for row in results:
                    #域名ID
                    domainid    = row.get('DNI_ID')
                    #域名
                    domainname  = row.get('DNI_DNAME')
                    #客户ID
                    domaincusid = row.get('DNI_CUSID')
                    #域名类型，web，download，media
                    domaintype  = row.get('DNI_TYPE')
                    #初始化归类的客户id和域名并转化为database和table
                    if domaintype == 1:
                        #WEB表名
                        table = "bandwidth_web"
                    elif domaintype == 3:
                        #MEDIA表名
                        table = "bandwidth_media"
                    elif domaintype == 2:
                        #DOWNLOAD表名
                        table = "bandwidth_download"
                    #拼接客户库名
                    database = 'data'+str(domaincusid)
                    #字典对象
                    value = {'domainid':str(domainid), 'domainname':domainname, 'customid':database, 'domaintype':table}
                    sqlconf_sem.acquire()
                    oth = sqlconf_dict.get(str(domainname), None)
                    if not oth:
                        sqlconpool_sem.acquire()
                        value = sqlconpool_dict.get(str(database), None)
                        if not value:
                            #创建数据库连接池
                            try:
                                sqlclient = Mysql(config.SQLHOST, config.SQLPORT, config.SQLUSER, config.SQLPASSWD, database, log)
                                sqlconpool_dict.update({str(database):sqlclient})
                            except:
                                info = sys.exc_info()
                                traceback.extract_tb(info[2])
                                log.error("update_wsdlconfig %s %s %s", info[0], info[1], sql)
                                sqlconpool_sem.release()
                                sqlconf_sem.release()
                                continue
                        else:
                            sqlconpool_sem.release()
                            sqlconf_sem.release()
                            continue
                        sqlconpool_sem.release()
                        #添加客户信息列表
                        sqlconf_dict.update({str(domainname):value})
                    sqlconf_sem.release()
                #初始化没有归类的域名和客户id
                value = {'domainid':'0', 'domainname':'qita', 'customid':'data0', 'domaintype':"bandwidth_qita"}
                sqlconf_sem.acquire()
                oth = sqlconf_dict.get('qita', None)
                if not oth:
                        #将没有记录的域名信息都归类到此库中去
                        sqlconf_dict.update({'qita':value})
                sqlconf_sem.release()
                sqlconpool_sem.acquire()
                value = sqlconpool_dict.get('data0', None)
                if not value:
                    #创建数据库连接池
                    try:
                        sqlclient = Mysql(config.SQLHOST, config.SQLPORT, config.SQLUSER, config.SQLPASSWD, 'data0', log)
                        sqlconpool_dict.update({'data0':sqlclient})
                    except:
                        info = sys.exc_info()
                        traceback.extract_tb(info[2])
                        log.error("update_wsdlconfig %s %s %s", info[0], info[1], sql)
                sqlconpool_sem.release()
                """
                   初始化没有归类的域名和客户id
                """
            gevent.sleep(60*10)

    def get_weninfo():
        
        try:
            #WEBSERVER接口取客户信息域名信息域名类型
            results = soapclient.service.cacheDateBaseBeans()
        except:
            info = sys.exc_info()
            traceback.extract_tb(info[2])
            log.error("update_wsdlconfig %s %s", info[0], info[1])
            return -1
        # 服务域名相关信息
        dir_sqlinfo = {}

        # 非泛域名需要增加的服务信息
        dir_sqladdinfo = {}
        # 非泛域名需要删除的域名信息
        list_sqldelinfo =[]
        # 非泛域名需要修改的服务信息
        dir_sqlmodinfo = {}

        # 泛域名需要增加的服务信息
        dir_sqlextensiveaddinfo = {}
        # 泛域名需要删除的域名信息
        list_sqlextensivedelinfo =[]
        # 泛域名需要修改的服务信息
        dir_sqlextensivemodinfo = {}
        # 获取服务端信息
        if results and len(results.DataBaseBean) > 0:
            for row in results.DataBaseBean:
                #域名ID;域名;数据库名;域名类型，web，download，media;数据库服务器ip;数据库服务器端口;数据库服务器用户名;数据库服务器密码
                #初始化归类的客户id和域名并转化为database和table
                str_domainname = str(row.domainname)
                value = {'domainid':str(row.domainId), 'domainname':str_domainname, 'customid':str(row.cusStr), 'domaintype':str(row.domaintype), 'db_ip':str(row.ip), 'db_port':int(row.port), 'db_name':str(row.name), 'db_pass':str(row.password)}
                log.debug("get cacheDateBaseBeans %s", str(value))
                # 泛域名
                if -1 != str_domainname.find("*"):
                    strdomainKey = "^" + str_domainname.replace(".","\.").replace("*","[\w\d\.\:\-\_]+") + "$"
                    # 检测增加的和修改的泛域名
                    updateAndaddinfo(strdomainKey,value,sqlextensive_dict,dir_sqlextensiveaddinfo,dir_sqlextensivemodinfo)
                # 非泛域名
                else:
                    strdomainKey = str_domainname
                    # 检测增加的和修改的一般域名
                    updateAndaddinfo(strdomainKey,value,sqlconf_dict,dir_sqladdinfo,dir_sqlmodinfo)
                # 生成服务信息字典以供对比
                dir_sqlinfo.update({str(strdomainKey):value})

            # 泛域名检查是否需要删除多余的域名
            deleteinfo(dir_sqlinfo,sqlextensive_dict,list_sqlextensivedelinfo)
            # 非泛域名检查是否需要删除多余的域名
            deleteinfo(dir_sqlinfo,sqlconf_dict,list_sqldelinfo)

            # 更新泛域名数据
            updatesqlinfo(sqlextensive_dict,dir_sqlextensiveaddinfo,dir_sqlextensivemodinfo,list_sqlextensivedelinfo)
            # 更新非泛域名数据
            updatesqlinfo(sqlconf_dict,dir_sqladdinfo,dir_sqlmodinfo,list_sqldelinfo)
            return 0
        return -1

    def update_wsdlconfig():
        log.debug("update_wsdl  service start")
        #初始化客户id和域名id
        while 1:
            if get_weninfo():
                log.debug("update web info error")
            else:
                log.debug("update web info succeed")
            gevent.sleep(60*60)
            
    def updateAndaddinfo(domainkey,dir_value,dir_memsqlinfo,dir_addinfo,dir_modinfo):
        #域名ID
        domainid = dir_value.get("domainid")
        #域名
        domainname = dir_value.get("domainname")
        #数据库名
        database = dir_value.get("customid")
        #域名类型，web，download，media
        domaintype = dir_value.get("domaintype")
        #数据库服务器ip
        database_ip = dir_value.get("db_ip")
        #数据库服务器端口
        database_port = dir_value.get("db_port")
        #数据库服务器用户名
        database_name = dir_value.get("db_name")
        #数据库服务器密码
        database_pass = dir_value.get("db_pass")

        oth = dir_memsqlinfo.get(str(domainkey), None)
        if not oth:
            # 添加到新增域名信息
            dir_addinfo.update({str(domainkey):dir_value})
        else:
            old_domainid    = oth.get('domainid')
            old_domaintype  = oth.get('domaintype')
            old_customid    = oth.get('customid')
            old_dbip        = oth.get('db_ip')
            old_dbport      = oth.get('db_port')
            old_dbname      = oth.get('db_name')
            old_dbpass      = oth.get('db_pass')
            if old_domainid != domainid or old_customid != database or old_dbip != database_ip or old_dbport != database_port or old_dbname != database_name or old_dbpass != database_pass:
                # 添加到修改域名信息
                dir_modinfo.update({str(domainkey):dir_value})

    def deleteinfo(dir_webinfo,dir_memsqlinfo,list_delinfo):
        # 遍历内存中的域名配置信息
        for sqlconf_key,sqlconf_values in dir_memsqlinfo.items():
            # 比对信息
            oth = dir_webinfo.get(str(sqlconf_key), None)
            if not oth:
                # 删除服务信息
                list_delinfo.append(sqlconf_key)

    def updatesqlinfo(dir_memsqlinfo,dir_addinfo,dir_modinfo,list_delinfo):
        strDomainname = "qita"
        # 删除无效数据
        if list_delinfo:

            loginfodel = ''.join(list_delinfo)
            log.debug("update_wsdlconfig[del %s]", loginfodel)

            sqlconf_sem.acquire()
            for delinfo in list_delinfo:
                # 删除域名信息
                if delinfo in dir_memsqlinfo.keys():
                    dir_memsqlinfo.pop(delinfo)
            sqlconf_sem.release()

        # 添加域名信息
        if dir_addinfo:
            sqlconf_sem.acquire()
            # 添加域名信息
            dir_memsqlinfo.update(dir_addinfo)
            sqlconf_sem.release()
            # 记录LOG信息
            loginfoadd = ''.join(dir_addinfo.keys())
            log.debug("update_wsdlconfig[add %s]", loginfoadd)

            #更新增加域名数据库连接信息(只更新qita域名) 
            if strDomainname in dir_addinfo.keys():
                # 添加数据sql链接
                sqlconpool_sem.acquire()  
                try:
                    # 获取数据
                    database_ip = dir_addinfo[strDomainname].get("db_ip")
                    database_port = dir_addinfo[strDomainname].get("db_port")
                    database_name = dir_addinfo[strDomainname].get("db_name")
                    database_pass = dir_addinfo[strDomainname].get("db_pass")
                    database = dir_addinfo[strDomainname].get("customid")
                    datadomaintype = dir_addinfo[strDomainname].get('domaintype')
                    sqlclient = Mysql(str(database_ip), int(database_port), str(database_name), str(database_pass), str(database), log)
                    sqlconpool_dict.update({str(strDomainname):[sqlclient,str(datadomaintype)]})
                except:
                    info = sys.exc_info()
                    traceback.extract_tb(info[2])
                    log.error("update_wsdlconfig addinfo%s %s", info[0], info[1])
                sqlconpool_sem.release()

        # 修改域名信息
        if dir_modinfo:
            # LOG信息记录
            loginfomod = ''.join(dir_modinfo.keys())
            log.debug("update_wsdlconfig[modify %s]", loginfomod)

            sqlconf_sem.acquire()
            # 修改域名信息
            dir_memsqlinfo.update(dir_modinfo)
            sqlconf_sem.release()
            #更新增加域名数据库连接信息(只更新qita域名) 
            if strDomainname in dir_modinfo.keys():
                # 添加数据sql链接
                sqlconpool_sem.acquire()  
                try:
                    # 获取数据
                    database_ip = dir_addinfo[strDomainname].get("db_ip")
                    database_port = dir_addinfo[strDomainname].get("db_port")
                    database_name = dir_addinfo[strDomainname].get("db_name")
                    database_pass = dir_addinfo[strDomainname].get("db_pass")
                    database = dir_addinfo[strDomainname].get("customid")
                    datadomaintype = dir_addinfo[strDomainname].get('domaintype')
                    sqlclient = Mysql(str(database_ip), int(database_port), str(database_name), str(database_pass), str(database), log)
                    sqlconpool_dict.update({str(strDomainname):[sqlclient,str(datadomaintype)]})
                except:
                    info = sys.exc_info()
                    traceback.extract_tb(info[2])
                    log.error("update_wsdlconfig addinfo%s %s", info[0], info[1])
                sqlconpool_sem.release()
                             
    def dispos_repqueue(dir_line):
        #日表的上传任务队列
        checkstring = "\n五分钟上报: \n"
        nInsertline = 0
        info = []

        for strDomain,strvalue in dir_line.items():
            for k,strDomainTime in strvalue.items():
                strCacheIP = eval(k)
                list_lineinfo = [strCacheIP["domain_time"],strDomain,strCacheIP["domain_ip"],strDomainTime]
                info.append(list_lineinfo)
                nInsertline += 1
            if nInsertline > 1000:
                nInsertline = 0
                #添加到上报队列
                tasks_repqueue.put(info)
                info = []
        if info:
            tasks_repqueue.put(info)

    def dispos_dayqueue():
        log.debug("dispos dayqueue service start")
        #日表的日志处理任务队列
        while 1:
            #当前时间戳
            timestamp = int(time.time())
            #调整时间戳以单位为准
            day_timestamp = timestamp / (60*5) * (60*5)
            sleep_time = (60*5) - (timestamp - day_timestamp)
            tasks_daysem.acquire()
            #存在需要上报的数据
            if tasks_daydict.keys():
                #添加到上报队列中
                dispos_repqueue(tasks_daydict)             
            #清楚上一个节点的缓存数据
            tasks_daydict.clear()
            tasks_daysem.release()
            #判断中断信号灯
            if is_sigint_up:
                #中断延迟，保证当前时间节点的数据全部上报完毕
                gevent.sleep(60*5)
                log.debug("interrupt signal dispos daylog exit")
                break
            #查询日志存储目录
            list_dirs = os.walk(config.LOGPATH)
            for root, dirs, files in list_dirs:
                if files:
                    for f in files:
                        #过滤日志
                        if not re.search('UPING', f) and not re.search('apflow.gz', f) and not re.search('s1.gz', f):
                             filename = os.path.join(root, f)
                             #添加到日志处理队列
                             tasks_dayqueue.put(filename)
            gevent.sleep(sleep_time)

    def historylog_queue():
        log.debug("upload history queue service start")
        #历史日表的日志处理任务队列
        while 1:
            #判断中断信号灯
            if is_sigint_up:
                log.debug("interrupt signal historylog exit")
                break
            #查询历史日志存储目录
            list_dirs = os.walk(config.HISTORYLOGPATH)
            for root, dirs, files in list_dirs:
                if files:
                    for f in files:
                        #过滤日志
                        if re.search('back', f):
                             filename = os.path.join(root, f)
                             #添加到历史日志处理队列
                             tasks_historyqueue.put(filename)
            gevent.sleep(60*5)

    def init_dayworkers():
        #创建线程定时去更新客户表域名等信息
        gevent.spawn(update_wsdlconfig)

        # 创建线程处理文件
        for i in range(config.THREAD_NUM):
            log.debug("http day worker thread %d start", i)
            worker = CacheTrafficDayWorker(config, log, i)
            gevent.spawn(worker.handle_tasks)
        # 创建线程处理上传失败的sql语句
        worker = CacheTrafficHistoryWorker(config, log)
        gevent.spawn(worker.handle_tasks)

        #五分钟线程上报日志处理
        worker = CacheTrafficRepWorker(config, log)
        gevent.spawn(worker.handle_tasks)

        #定时扫描历史未能上报成功的日志
        gevent.spawn(historylog_queue)
        
        #定时扫描日志
        dispos_dayqueue()

    def init_soap():
        loop = 0
        while True:
            try:
                #创建WEBSERVER交互的接口
                pgram, uri = re.split("//", config.WSDL)
                domain= re.split("/", uri)[0]
                path = uri[len(domain):len(uri)]
                if re.search(':', domain):
                    host, port = re.split(":", domain)
                    #解析域名到IP的过程
                    ipresults = socket.getaddrinfo(host, None, 0, socket.SOCK_STREAM)
                    af, socktype, proto, canonname, sa = ipresults[0]
                    #重组URL
                    url = pgram + '//' + sa[0] + ':' + port + path
                else:
                    host= re.split(":", domain)[0]
                    #解析域名到IP的过程
                    ipresults = socket.getaddrinfo(host, None, 0, socket.SOCK_STREAM)
                    af, socktype, proto, canonname, sa = ipresults[0]
                    #重组URL
                    url = pgram + '//' + sa[0] + path
                #工作域
                imp    = Import(config.WSDL_BEAN)
                imp.filter.add(config.WSDL_SERVICE)
                d      = ImportDoctor(imp)
                #客户端接口
                client = Client(url,doctor=d)
            except:
                #出现异常， 写入日志
                if loop > 10:
                    info = sys.exc_info()
                    traceback.extract_tb(info[2])
                    logs.error("%s %s", info[0], info[1])
                    return 0
                else:
                    loop += 1
                    continue
            return client

    reload(sys)
    sys.setdefaultencoding('utf-8')

    #创建配置接口
    config = loadconfig()
    #创建日志接口
    log = loginit()
    #中断信号处理
    signal.signal(signal.SIGQUIT, sigint_handler)
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGHUP, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    is_sigint_up = False
    #链接WEBSERVER
    soapclient = init_soap()
    """
    sqlconfclient = Mysql(config.SQLHOST, config.SQLPORT, config.SQLUSER, config.SQLPASSWD, config.SQLDATABASE, log)
    """
    # 加载配置
    if get_weninfo():
        log.error("CacheTraffic start failed,check WSDL_SERVICE in config!")
        sys.exit()
    #开启日志处理
    init_dayworkers()

