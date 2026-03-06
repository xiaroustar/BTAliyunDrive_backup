#!/usr/bin/env python
# encoding: utf-8
import sqlite3
import os
import time
import sys

basedir = os.path.abspath(os.path.dirname(__file__))
setup_path = '/www/server/panel/plugin/aliyundrive_backup'
os.chdir(basedir)


class Sql():
    """
    轻量版 sqlite 封装，用于在插件内保存站点备份配置等信息。
    说明：
    - 数据库文件：/www/server/panel/plugin/aliyundrive_backup/aliyundrive_backup.db
    - 本插件主要使用的表：
        web(sites_id INTEGER PRIMARY KEY, name TEXT, path TEXT, create_time TEXT)
    """
    __DB_FILE = None
    __DB_CONN = None
    __DB_TABLE = ""
    __OPT_WHERE = ""
    __OPT_LIMIT = ""
    __OPT_ORDER = ""
    __OPT_FIELD = "*"
    __OPT_PARAM = ()

    def __init__(self):
        self.__DB_FILE = os.path.join(setup_path, 'aliyundrive_backup.db')
        self._ensure_db()

    def _ensure_db(self):
        # 初始化：保证 web、log、database 和 conf 表存在
        conn = sqlite3.connect(self.__DB_FILE)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS web (
                sites_id INTEGER PRIMARY KEY,
                name TEXT,
                path TEXT,
                create_time TEXT
            );
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                content TEXT,
                create_time TEXT
            );
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS database (
                databases_id INTEGER PRIMARY KEY,
                name TEXT,
                ps TEXT,
                create_time TEXT
            );
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS conf (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE,
                val TEXT
            );
        """)
        conn.commit()
        conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_trackback):
        self.close()

    def __GetConn(self):
        """
        获取数据库连接。
        约定：
        - 成功返回 True
        - 失败返回 "error: xxx"
        """
        try:
            if self.__DB_CONN is None:
                # 确保数据库目录存在，避免 sqlite3.connect 因目录不存在而失败
                db_dir = os.path.dirname(self.__DB_FILE)
                if db_dir and (not os.path.exists(db_dir)):
                    os.makedirs(db_dir, exist_ok=True)
                self.__DB_CONN = sqlite3.connect(self.__DB_FILE)
                self.__DB_CONN.text_factory = str
            return True
        except Exception as ex:
            # 确保连接保持为 None，避免后续访问 None.text_factory
            self.__DB_CONN = None
            return "error: " + str(ex)

    def table(self, table):
        self.__DB_TABLE = table
        return self

    def where(self, where, param):
        if where:
            self.__OPT_WHERE = " WHERE " + where
            if type(param) != tuple:
                param = (param,)
            self.__OPT_PARAM = param
        return self

    def field(self, field):
        if len(field):
            self.__OPT_FIELD = field
        return self

    def order(self, order):
        """ORDER条件"""
        if len(order):
            self.__OPT_ORDER = " ORDER BY " + order
        return self

    def limit(self, limit):
        if limit:
            self.__OPT_LIMIT = " LIMIT " + str(limit)
        return self

    def __get_columns(self):
        """当字段为 * 时，自动获取表的所有列名"""
        if self.__OPT_FIELD == '*':
            tmp_cols = self.query('PRAGMA table_info(' + self.__DB_TABLE + ')', ())
            cols = []
            for col in tmp_cols:
                if len(col) > 2:
                    cols.append('`' + col[1] + '`')
            if len(cols) > 0:
                self.__OPT_FIELD = ','.join(cols)

    def select(self):
        conn_res = self.__GetConn()
        if isinstance(conn_res, str) and conn_res.startswith("error:"):
            return conn_res
        try:
            self.__get_columns()
            sql = "SELECT " + self.__OPT_FIELD + " FROM " + self.__DB_TABLE + self.__OPT_WHERE + self.__OPT_ORDER + self.__OPT_LIMIT
            result = self.__DB_CONN.execute(sql, self.__OPT_PARAM)
            data = result.fetchall()
            # 构造成列表[dict]
            if self.__OPT_FIELD != "*" and not self.__OPT_FIELD.startswith('`'):
                keys = [k.strip().strip('`') for k in self.__OPT_FIELD.split(',')]
                rows = []
                for row in data:
                    item = {}
                    for i, k in enumerate(keys):
                        item[k] = row[i]
                    rows.append(item)
                data = rows
            else:
                # 当使用 * 时，需要从列信息构造字典
                if self.__OPT_FIELD.startswith('`'):
                    keys = [k.strip('`') for k in self.__OPT_FIELD.split(',')]
                    rows = []
                    for row in data:
                        item = {}
                        for i, k in enumerate(keys):
                            item[k] = row[i]
                        rows.append(item)
                    data = rows
                else:
                    data = [list(r) for r in data]
            self.__close()
            return data
        except Exception as ex:
            return "error: " + str(ex)

    def get(self):
        """获取所有数据（自动处理列名）"""
        self.__get_columns()
        return self.select()

    def query(self, sql, param=()):
        """执行SQL语句返回数据集"""
        conn_res = self.__GetConn()
        if isinstance(conn_res, str) and conn_res.startswith("error:"):
            return conn_res
        try:
            if type(param) != tuple:
                param = (param,)
            result = self.__DB_CONN.execute(sql, param)
            data = list(map(list, result))
            return data
        except Exception as ex:
            return "error: " + str(ex)

    def find(self):
        res = self.limit("1").select()
        if isinstance(res, list) and len(res) == 1:
            return res[0]
        return res

    def add(self, keys, param):
        conn_res = self.__GetConn()
        if isinstance(conn_res, str) and conn_res.startswith("error:"):
            return conn_res
        try:
            values = ",".join(["?"] * len(keys.split(',')))
            sql = "INSERT INTO " + self.__DB_TABLE + "(" + keys + ") VALUES(" + values + ")"
            result = self.__DB_CONN.execute(sql, param if isinstance(param, tuple) else (param,))
            row_id = result.lastrowid
            self.__close()
            self.__DB_CONN.commit()
            return row_id
        except Exception as ex:
            return "error: " + str(ex)

    def delete(self, id=None):
        """删除数据，支持按 id 或使用 where 条件"""
        conn_res = self.__GetConn()
        if isinstance(conn_res, str) and conn_res.startswith("error:"):
            return conn_res
        try:
            if id:
                # 如果提供了 id，覆盖 where 条件
                self.__OPT_WHERE = " WHERE id=?"
                self.__OPT_PARAM = (id,)
            elif not self.__OPT_WHERE:
                # 如果没有 where 条件也没有 id，报错
                self.__close()
                return "error: 删除操作需要提供 where 条件或 id"
            sql = "DELETE FROM " + self.__DB_TABLE + self.__OPT_WHERE
            result = self.__DB_CONN.execute(sql, self.__OPT_PARAM)
            rowcount = result.rowcount
            self.__close()
            self.__DB_CONN.commit()
            return rowcount
        except Exception as ex:
            self.__close()
            return "error: " + str(ex)

    def __close(self):
        self.__OPT_WHERE = ""
        self.__OPT_FIELD = "*"
        self.__OPT_ORDER = ""
        self.__OPT_LIMIT = ""
        self.__OPT_PARAM = ()

    def close(self):
        try:
            if self.__DB_CONN:
                self.__DB_CONN.close()
                self.__DB_CONN = None
        except Exception:
            pass

