#!/usr/bin/env python
#
# Copyright 2012 fkook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A lightweight wrapper around MySQLdb."""

import copy
import MySQLdb
import MySQLdb.constants
import MySQLdb.converters
import MySQLdb.cursors
import itertools
import logging
from time import time

class Connection(object):
    """A lightweight wrapper around MySQLdb DB-API connections.

    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage:

        db = database.Connection("localhost", "mydatabase")
        for article in db.query("SELECT * FROM articles"):
            print article.title

    Cursors are hidden by the implementation, but other than that, the methods
    are very similar to the DB-API.

    We explicitly set the timezone to UTC and the character encoding to
    UTF-8 on all connections to avoid time zone and encoding errors.
    """
    def __init__(self, host, database, user=None, password=None):
        self.host = host
        self.database = database

        args = dict(conv=CONVERSIONS, use_unicode=True, charset="utf8",
                    db=database, init_command='SET time_zone = "+8:00"',
                    sql_mode="TRADITIONAL")
        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306

        self._db = None
        self._db_args = args
        try:
            self.reconnect()
        except:
            logging.error("Cannot connect to MySQL on %s", self.host,
                          exc_info=True)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def commit(self):
        if self._db is not None:
            try:
                self._db.ping()
            except:
                self.reconnect()
            try:
                self._db.commit()
            except Exception,e:
                self._db.rollback()
                logging.exception("Can not commit",e)

    def rollback(self):
        if self._db is not None:
            try:
                self._db.rollback()
            except Exception,e:
                logging.error("Can not rollback")

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = MySQLdb.connect(**self._db_args)
        self._db.autocommit(False)


    def iter(self, query, *parameters):
        """Returns an iterator for the given query and parameters."""
        if self._db is None: self.reconnect()
        cursor = MySQLdb.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query, *parameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(itertools.izip(column_names, row)) for row in cursor]
        finally:
            cursor.close()



    def get(self, query, *parameters):
        """Returns the first row returned for the given query."""
        rows = self.query(query, *parameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    def execute(self, query, *parameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()
    def count(self,query, *parameters):
        """Executes the given query, returning the count value from the query."""
        cursor = self._cursor()
        try:
            cursor.execute(query, parameters)
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def __getattr__(self,tablename):
        '''
        return single table queryer for select table
        '''
        return TableQueryer(self,tablename)
    
    def fromQuery(self,Select):
        '''
        return single table queryer for query
        '''
        return TableQueryer(self,Select)

    def insert(self,table,**datas):
        '''
        Executes the given parameters to an insert SQL and execute it
        '''
        return Insert(self,table)(**datas)

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def _cursor(self):
        if self._db is None: self.reconnect()
        try:
            self._db.ping()
        except:
            self.reconnect()
        return self._db.cursor()

    def _execute(self, cursor, query, parameters):
        try:
            return cursor.execute(query, parameters)
        except OperationalError:
            logging.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise

class TableQueryer:
    '''
    Support for single table simple querys
    '''
    def __init__(self,db,tablename):
        self.tablename=tablename
        self.db=db

    def get_one(self,query):
        rs=Select(self.db,self.tablename,query)()
        if len(rs)>0:
            if len(rs)>1:
                raise OperationalError,"returned multi row when fetch one result"
            return rs[0]
        return None

    def insert(self,**fields):
        return Insert(self.db,self.tablename)(**fields)

    def __call__(self,query=None):
        return Operater(self.db,self.tablename,query)

    def __getattr__(self,field_name):
        return conds(field_name)

class Operater:
    def __init__(self,db,tablename,query):
        self.insert=Insert(db,tablename)
        self.count=Count(db,tablename,query)
        self.select=Select(db,tablename,query)
        self.update=Update(db,tablename,query)
        self.delete=Delete(db,tablename,query)

class Count (object):
    '''
    Count with current where clouse
    '''
    def __init__(self,db,tablename,where):
        self.db=db
        self.tablename=tablename
        self.where=where

    def __call__(self):
        if self.where:
            _sql="".join(["SELECT count(1) FROM ",self.tablename," where ",self.where.get_sql()])
            _params=self.where.get_params()
            return self.db.count(_sql,*_params)
        else:
            _sql="",join(["SELECT count(1) FROM ",self.tablename])
            return self.db.count(_sql)

class Select:
    '''
    Select list with current where clouse 
    '''
    def __init__(self,db,tablename,where):
        self.db=db
        self._tablename=tablename
        self._where=where
        self._sort_fields=[]
        self._limit=None
        self._fields=[]
        self._groups=[]
        self._having=None
        
    def sort(self,**fields):
        del self._sort_fields[:]
        for key in fields.keys():
            self._sort_fields.append("".join(["`",key,"` ",fields[key]]))
        return self

    def limit(self,start,count):
        self._limit="".join(["LIMIT ",str(start),",",str(count)])
        return self

    def collect(self,*fields):
        if len(fields):
            self._fields+=fields
        return self        

    def group_by(self,*fields):
        if len(fields)<1:
            raise OperationalError,"Must have a field"
        for f in fields:
            self._groups.append(f)

    def having(self,cond):
        self._having=cond

    def __getslice__(self,pid,pcount):
        if pid<1:
            raise OperationalError,"Wrong page id,page id can not lower than 1"
        if pcount<1:
            raise OperationalError,"Wrong page size,page size can not lower than 1"
        _start=(pid-1)*pcount
        self._limit="".join(["LIMIT ",str(_start),",",str(pcount)])
        return self

    def get_sql(self):
        _sql_slice=["SELECT "]
        if self._fields:
            _sql_slice.append(",".join(["".join(["`",str(f),"`"]) for f in self._fields]))
        else:
            _sql_slice.append("*")
        _sql_slice.append(" FROM `")
        if str(self._tablename.__class__)=="database.Select":
            _sql_slice.append("(")
            _sql_slice.append(self._tablename.get_sql())
            _sql_slice.append(")t")
        else:
            _sql_slice.append(self._tablename)
        _sql_slice.append("`")
        if self._where:
            _sql_slice.append(" WHERE ")
            if str(self._tablename.__class__)=="database.Select":
                _sql_slice.append(self._where.get_sql(tn='t'))
            else:
                 _sql_slice.append(self._where.get_sql())
            _sql_slice.append(" ")
        if len(self._groups)>0:
            _sql_slice.append("GROUP BY ")
            if str(self._tablename.__class__)=="database.Select":
                _sql_slice.append(",".join([f.get_sql(tn="t") for f in self._groups]))
            else:
                _sql_slice.append(",".join([f.get_sql() for f in self._groups]))
            if self._having:
                _sql_slice.append(" HAVING ")
                _sql_slice.append(self._having.get_sql())
                _sql_slice.append(" ")
        if self._sort_fields:
            _sql_slice.append("ORDER BY ")
            if str(self._tablename.__class__)=="database.Select":
                _sql_slice.append(",".join([self._add_tb('t',s) for s in self._sort_fields]))
            else:
                _sql_slice.append(",".join([s for s in self._sort_fields]))
        if self._limit:
            _sql_slice.append(" ")
            _sql_slice.append(self._limit)
        return "".join(_sql_slice)
    
    def _add_tb(self,tn,src):
        import re
        p=compile(r'`(\w*?)`')
        return p.sub(r'`%s.\1`'%tn,src)

    def __call__(self):
        _sql=self.get_sql()
        _plist=[]
        if str(self._tablename.__class__)=="database.Select":
            for p in self._tablename.get_sql():
                _plist.append(p)
        if self._where:
            for p in self._where:
                _plist.append(p)
        if self._having:
            for p in self._having.get_params():
                _plist.append(p)
        if _plist:
            return self.db.query(_sql,*_plist)
        else:
            return self.db.query(_sql)

class Update:
    '''
    Update Query Generator
    '''
    def __init__(self,db,tablename,where):
        self.db=db
        self._tablename=tablename
        self._where=where
        self._update_cols=None
    
    def __call__(self,*fields):
        if len(fields)<1:
            raise OperationalError,"Must have unless 1 field to update"
        _params=[]
        _cols=[]
        for f in fields:
            _cols.append(f.get_sql())
            _params.append(f.get_params()[0])
        _sql_slice=["UPDATE ",self._tablename," SET ",",".join(_cols)]
        if self._where:
            _sql_slice.append(" WHERE ")
            _sql_slice.append(self._where.get_sql())
            for p in self._where.get_params():
                _params.append(p)
        _sql="".join(_sql_slice)
        return self.db.execute(_sql,*_params)

class Delete:
    def __init__(self,db,tablename,where):
        self.db=db
        self._tablename=tablename
        self._where=where

    def __call__(self):
        _sql_slice=["DELETE FROM `",self._tablename,"`"]
        if self._where:
            _sql_slice.append(" WHERE ")
            _sql_slice.append(self._where.get_sql())
            _sql="".join(_sql_slice)
            return self.db.execute(_sql,self._where.get_params())

class Insert:
    '''
    Insert Query Generator
    '''
    def __init__(self,db,tablename):
        self.db=db
        self.tablename=tablename

    def __call__(self,**fileds):
        columns=fileds.keys()
        _prefix="".join(['INSERT INTO `',self.tablename,'`'])
        _fields=",".join(["".join(['`',column,'`']) for column in columns])
        _values=",".join(["%s" for i in range(len(columns))])
        _sql="".join([_prefix,"(",_fields,") VALUES (",_values,")"])
        _params=[fileds[key] for key in columns]
        return self.db.execute(_sql,*tuple(_params))

class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class conds:
    def __init__(self,field):
        self.field_name=field
        self._sql=""
        self._params=[]
        self._has_value=False
        self._sub_conds=[]
        self._no_value=False

    def _prepare(self,sql,value):
        if not self._has_value:
            self._sql=sql
            self._params.append(value)
            self._has_value=True
            return self
        raise OperationalError,"Multiple Operate conditions"

    def __str__(self):
        if self._has_value:
            return self._sql
        else:
            return self.field_name

    def get_sql(self,tn=None):
        _sql_slice=[]
        _sql_slice.append(self._sql)
        _sql_slice.append(" ")
        if len(self._sub_conds):
            for cond in self._sub_conds:
                _sql_slice.append(cond[1])
                _sql_slice.append(cond[0].get_sql()) 
        _where = "".join(_sql_slice)
        if tn:
            import re
            p=compile(r'`(\w*?)`')
            _where = p.sub(r'`%s.\1`'%tn,_where)
        return _where

    def get_params(self):
        _my_params=[]+self._params
        if len(self._sub_conds):
            for cond in self._sub_conds:
                _my_params+=cond[0].get_params()
        return _my_params

    def __sub__(self,value):
        return self._prepare("".join(["`",self.field_name,"`-%s"]),value)

    def __add__(self,value):
        return self._prepare("".join(["`",self.field_name,"`+%s"]),value)

    def __ne__(self,value):
        return self._prepare("".join(["`",self.field_name,'`','<>%s']),value)

    def __eq__(self,value):
        if not self._has_value:
            if str(value.__class__)=="database.conds":
                self._sql="".join(["`",self.field_name,'`','=',value.get_sql()])
                self._params.append(value.get_params()[0])
            else:
                self._sql="".join(["`",self.field_name,'`','=%s'])
                self._params.append(value)
            self._has_value=True
            return self
        raise OperationalError,"Multiple Operate conditions"

    def like(self,value):
        return self._prepare("".join(["`",self.field_name,'`',' like %s']),value)

    def DL(self,format,value):
        return self._prepare("".join(["DATE_FORMAT(`",self.field_name,"`,'",format,"')",'<=%s']),value)

    def DG(self,format,value):
        return self._prepare("".join(["DATE_FORMAT(`",self.field_name,"`,'",format,"')",'>=%s']),value) 

    def DE(self,format,value):
        return self._prepare("".join(["DATE_FORMAT(`",self.field_name,"`,'",format,"')",'=%s']),value)

    def __le__(self,value):
        return self._prepare("".join(["`",self.field_name,'`','<=%s']),value)

    def __lt__(self,value):
        return self._prepare("".join(["`",self.field_name,'`','<%s']),value)

    def __gt__(self,value):
        return self._prepare("".join(["`",self.field_name,'`','>%s']),value)

    def __ge__(self,value):
        return self._prepare("".join(["`",self.field_name,'`','>=%s']),value)

    def In(self,array):
        if not self._has_value:
            if str(array.__class__)=="database.Select":
                self._sql="".join(["`",self.field_name,'`',' in (',array.get_sql(),")"])
                for p in array.get_params():
                    self._params.append(p)
            else:
                _values=",".join(["".join(['\'',i,'\'']) for i in array])
                self._sql="".join(["`",self.field_name,'`',' in (',_values,")"])
                self._has_value=True
            return self
        raise OperationalError,"Multiple Operate conditions"
    
    def Not_In(self,array):
        if not self._has_value:
            if str(array.__class__)=="database.Select":
                self._sql="".join(["`",self.field_name,'`',' not in (',array.get_sql(),")"])
                for p in array.get_params():
                    self._params.append(p)
            else:
                _values=",".join(["".join(['\'',i,'\'']) for i in array])
                self._sql="".join(["`",self.field_name,'`',' not in (',_values,")"])
                self._has_value=True
            return self
        raise OperationalError,"Multiple Operate conditions"

    def __getattr__(self,func_name):
        if not self._has_value:
            if str(array.__class__)=="database.Select":
                self.self.field_name="".join([func_name,"(t.",self.field_name,") as ",func_name,"_",self.field_name])
            else:
                self.self.field_name="".join([func_name,"(",self.field_name,") as ",func_name,"_",self.field_name])
            return self
        raise OperationalError,"Multiple Operate conditions"

    def __and__(self,cond):
        if self._has_value:
            self._sub_conds.append((cond," AND "))
            return self
        raise OperationalError,"Operation with no value"

    def __or__(self,cond):
        if self._has_value:
            self._sub_conds.append((cond," OR "))
            return self
        raise OperationalError,"Operation with no value"

# Fix the access conversions to properly recognize unicode/binary
FIELD_TYPE = MySQLdb.constants.FIELD_TYPE
FLAG = MySQLdb.constants.FLAG
CONVERSIONS = copy.deepcopy(MySQLdb.converters.conversions)
for field_type in \
        [FIELD_TYPE.BLOB, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING] + \
        ([FIELD_TYPE.VARCHAR] if 'VARCHAR' in vars(FIELD_TYPE) else []):
    CONVERSIONS[field_type].insert(0, (FLAG.BINARY, str))


# Alias some common MySQL exceptions
IntegrityError = MySQLdb.IntegrityError
OperationalError = MySQLdb.OperationalError
