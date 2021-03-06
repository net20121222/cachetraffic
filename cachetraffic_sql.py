#!/usr/bin/python
#coding:utf-8

import MySQLdb
from MySQLdb.cursors import DictCursor
from DBUtils.PooledDB import PooledDB

class Mysql(object):
    __pool = None
    def __init__(self, host, port, user, passwd, database, log):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.database = database
        self.log = log
        self._conn = Mysql.__getConn(self.host, self.port, self.user, self.passwd, self.database)
        self._cursor = self._conn.cursor()

    @staticmethod
    def __getConn(ihost, iport, iuser, ipasswd, idatabase):
        if Mysql.__pool is None:
            __pool = PooledDB(creator=MySQLdb, mincached=1 , maxcached=3 ,
                              host=ihost , port=iport , user=iuser , passwd=ipasswd,
                              db=idatabase, use_unicode=False,cursorclass=DictCursor)
        return __pool.connection()

    def getAll(self,sql,param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql,param)
        if count>0:
            result = self._cursor.fetchall()
        else:
            result = False
        return result

    def getOne(self,sql,param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql,param)
        if count>0:
            result = self._cursor.fetchone()
        else:
            result = False
        return result
 
    def getMany(self,sql,num,param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql,param)
        if count>0:
            result = self._cursor.fetchmany(num)
        else:
            result = False
        return result

    def insertOne(self,sql,value):
        self._cursor.execute(sql,value)
        return self.__getInsertId()
 
    def insertMany(self,sql,values):
        count = self._cursor.executemany(sql,values)
        return count
 
    def __getInsertId(self):
        self._cursor.execute("SELECT @@IDENTITY AS id")
        result = self._cursor.fetchall()
        return result[0]['id']
 
    def __query(self,sql,param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql,param)
        return count

    def update(self,sql,param=None):
        return self.__query(sql,param)
 
    def delete(self,sql,param=None):
        return self.__query(sql,param)
 
    def begin(self):
        self._conn.autocommit(0)
 
    def end(self,option='commit'):
        if option=='commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def dispose(self,isEnd=1):
        if isEnd==1:
            self.end('commit')
        else:
            self.end('rollback');
        self._cursor.close()
        self._conn.close()

