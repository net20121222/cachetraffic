#!/usr/bin/python
#coding:utf-8
import os, sys
import configuration

from configuration import Boolean, Integer, String, StringList


class CacheTrafficServerConfig(configuration.Config):
    LOGPATH = String("/home/flux", "log path")
    LOGPATH.comment_out_default = False
    HISTORYLOGPATH = String("/cache/history", "history log path")
    HISTORYLOGPATH.comment_out_default = False
    SQLHOST = String("118.244.210.8", "sql host")
    SQLHOST.comment_out_default = False
    SQLPORT = Integer(3306, "sql port")
    SQLPORT.comment_out_default = False
    SQLUSER = String("root", "sql user")
    SQLUSER.comment_out_default = False
    SQLPASSWD = String("yiyun", "sql passwd")
    SQLPASSWD.comment_out_default = False
    SQLDATABASE = String("bandwidthtest", "sql database")
    SQLDATABASE.comment_out_default = False
    BACKLOGPATH = String("/cache/back", "back log path")
    BACKLOGPATH.comment_out_default = False
    BACKERRORLOGPATH = String("/cache/error", "back errorlog path")
    BACKERRORLOGPATH.comment_out_default = False
    WSDL = String("http://service.exclouds.com:44100/CacheDateBaseWebService.cis?wsdl", "wsdl url")
    WSDL.comment_out_default = False
    WSDL_BEAN = String("http://bean.cdn.excloud.com", "wsdl bean")
    WSDL_BEAN.comment_out_default = False
    WSDL_SERVICE = String("http://services.spring.excloud.com", "wsdl services")
    WSDL_SERVICE.comment_out_default = False
    THREAD_NUM = Integer(50, "worker num")
    THREAD_NUM.comment_out_default = False
    TASK_RETRIGGER_TIME= Integer(10, "task retry num")
    TASK_RETRIGGER_TIME.comment_out_default = False
    VERSION = Integer(0, "Version of the config file")
    VERSION.comment_out_default = False

    def __init__(self, path):
        configuration.Config.__init__(self)
        self.dir = os.path.dirname(path)
        if os.path.exists(path):
            self.load(path)
            if self.VERSION != self.__class__.VERSION.default:
                self.VERSION = self.__class__.VERSION.default
                self.save(path)
        else:
            self.save(path)
        self.path = path

    def save(self, path=None):
        configuration.Config.save(self, path or self.path)

