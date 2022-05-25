#!/usr/bin/env python
# encoding: utf-8

import unittest
import os
import sqlite3

# try:
#     import mysql.connector
#     from mysql.connector import errorcode
#     mysql_connector_installed = True
# except ImportError:
#     mysql_connector_installed = False
import pymysql

from collections import OrderedDict

from wow.db_utils import (
    db_config_template,
    configure_db,
    write_to_db,
    _make_connection,
    read_db,
    update_to_db,
    finalise_db,
    check_mysql_database_exists,
    delete_from_db,
)

# @unittest.skipIf(not mysql_connector_installed, "MariaDB/MySQL connector is not installed so skipping MySQL/MariaDB tests")
class MariaDBUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_fields = OrderedDict(
            [
                ("UPRN", "INTEGER PRIMARY KEY"),
                ("PropertyID", "INT"),
                ("Addr1", "TEXT"),
            ]
        )
        test_root = os.path.dirname(__file__)
        cls.db_dir = os.path.join(test_root, "fixtures")

    def test_configure_mariadb(self):
        # Connect to engine and delete test table if it exists
        db_config = db_config_template.copy()
        db_config["db_name"] = "test"
        password = os.environ["MARIA_DB_PASSWORD"]
        port = int(os.getenv("MARIA_DB_PORT", "3306"))

        conn = pymysql.connect(host="127.0.0.1", user="root", password=password)

        cursor = conn.cursor()
        cursor.execute("DROP DATABASE IF EXISTS test")
        conn.commit()
        configure_db(db_config, self.db_fields, tables="test")

        # Test database exists
        try:
            conn.database = db_config["db_name"]
        except pymysql.Error as err:
            raise
        # Do a schema query
        conn.close()

        # Now we've created the database, query it for correct columns
        db_config = db_config_template.copy()
        db_config["db_name"] = "information_schema"
        sql_query = (
            "select COLUMN_NAME from COLUMNS where table_schema='test' and table_name='test'"
        )
        rows = read_db(sql_query, db_config)

        exp_columns = set([x for x in self.db_fields.keys()])
        obs_columns = set([x["COLUMN_NAME"] for x in rows])
        self.assertEqual(exp_columns, obs_columns)

    def test_write_to_mariadb(self):
        db_config = db_config_template.copy()
        db_config = configure_db(db_config, self.db_fields, tables="test", force=True)
        data = ((1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans"))
        write_to_db(data, db_config, self.db_fields, table="test")
        conn = _make_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(
            """
            select * from test;
        """
        )
        rows = cursor.fetchall()
        self.assertEqual(data, rows)

    def test_write_geom_to_mariadb(self):
        db_config = db_config_template.copy()

        db_fields = OrderedDict(
            [
                ("UPRN", "INTEGER PRIMARY KEY"),
                ("PropertyID", "INT"),
                ("points", "POINT"),
            ]
        )

        db_config = configure_db(db_config, db_fields, tables="test", force=True)
        data = [(1, 2, "POINT(0 10)"), (2, 3, "POINT(20 20)"), (3, 3, "POINT(5 15)")]

        expected = ((1, 2, 0.0, 10.0), (2, 3, 20.0, 20.0), (3, 3, 5.0, 15.0))

        write_to_db(data, db_config, db_fields, table="test")
        conn = _make_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(
            """
            select UPRN, PropertyID, X(points), Y(points) from test;
        """
        )
        rows = cursor.fetchall()
        self.assertEqual(expected, rows)

    def test_update_mariadb(self):
        db_config = db_config_template.copy()
        db_config = configure_db(db_config, self.db_fields, tables="test", force=True)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        write_to_db(data, db_config, self.db_fields, table="test")

        update_fields = ["Addr1", "UPRN"]
        update = [("Some", 3)]
        update_to_db(update, db_config, update_fields, table="test", key="UPRN")

        conn = _make_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(
            """
            select Addr1 from test where UPRN = 3 ;
        """
        )
        rows = cursor.fetchall()
        expected = ("Some",)
        self.assertEqual(expected, rows[0])

    def test_finalise_mariadb(self):
        db_config = db_config_template.copy()

        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_config, self.db_fields, tables="test", force=True)
        write_to_db(data, db_config, self.db_fields, table="test")
        finalise_db(db_config, index_name="idx_propertyID", table="test", colname="propertyID")

    def test_read_mariadb(self):
        db_config = db_config_template.copy()

        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_config, self.db_fields, tables="test", force=True)
        write_to_db(data, db_config, self.db_fields, table="test")

        sql_query = "select * from test;"

        for i, row in enumerate(read_db(sql_query, db_config)):
            test_data = OrderedDict(zip(self.db_fields.keys(), data[i]))
            self.assertEqual(row, test_data)

    def test_check_mysql_database_exists(self):
        db_config = db_config_template.copy()
        db_config["db_name"] = "djnfsjnf"

        self.assertEqual(check_mysql_database_exists(db_config), False)

        db_config["db_name"] = "INFORMATION_SCHEMA"
        self.assertEqual(check_mysql_database_exists(db_config), True)


class DatabaseUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_fields = OrderedDict(
            [
                ("UPRN", "INTEGER PRIMARY KEY"),
                ("PropertyID", "INT"),
                ("Addr1", "TEXT"),
            ]
        )
        test_root = os.path.dirname(__file__)
        cls.db_dir = os.path.join(test_root, "fixtures")

        # if os.path.isfile(cls.db_file_path):
        #    os.remove(cls.db_file_path)

    def test_configure_db(self):
        db_filename = "test_config_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        configure_db(db_file_path, self.db_fields, tables="test")
        # Test file exists
        self.assertEqual(True, os.path.isfile(db_file_path))
        # Do a schema query
        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute(
                """
                select * from test;
            """
            )
            exp_columns = set([x for x in self.db_fields.keys()])
            obs_columns = set([x[0] for x in cursor.description])
            self.assertEqual(exp_columns, obs_columns)

    def test_configure_db_pk_and_autoinc(self):
        db_filename = "test_config_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)

        mod_db_fields = self.db_fields.copy()
        mod_db_fields["UPRN"] = "INTEGER PRIMARY KEY"

        configure_db(db_file_path, mod_db_fields, tables="test")
        # Test file exists
        self.assertEqual(True, os.path.isfile(db_file_path))
        # Do a schema query
        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute(
                """
                select * from test;
            """
            )
            exp_columns = set([x for x in self.db_fields.keys()])
            obs_columns = set([x[0] for x in cursor.description])
            self.assertEqual(exp_columns, obs_columns)

    def test_configure_multi_db(self):
        db_filename = "test_config_multi_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)

        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        tables = ["test1", "test2"]
        db_fields2 = OrderedDict(
            [
                ("UPRN2", "INTEGER PRIMARY KEY"),
                ("PropertyID2", "INT"),
                ("Addr2", "TEXT"),
            ]
        )
        db_field_set = {"test1": self.db_fields, "test2": db_fields2}
        configure_db(db_file_path, db_field_set, tables=tables)
        # Test file exists
        self.assertEqual(True, os.path.isfile(db_file_path))
        # Do a schema query
        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            for i in range(0, 2):
                cursor.execute("select * from {};".format(tables[i]))
                exp_columns = set([x for x in db_field_set[tables[i]].keys()])
                obs_columns = set([x[0] for x in cursor.description])
                self.assertEqual(exp_columns, obs_columns)

    def test_write_to_db(self):
        db_filename = "test_write_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test")
        write_to_db(data, db_file_path, self.db_fields, table="test")
        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute(
                """
                select * from test;
            """
            )
            rows = cursor.fetchall()
            self.assertEqual(data, rows)

    def test_delete_from_db(self):
        db_filename = "test_delete_from_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test")
        write_to_db(data, db_file_path, self.db_fields, table="test")

        delete_from_db("""delete from test where uprn=1""", db_file_path)

        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute(
                """
                select * from test;
            """
            )
            rows = cursor.fetchall()
            self.assertEqual(data[1:], rows)

    def test_write_dictionaries_to_db(self):
        db_filename = "test_write_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [
            OrderedDict([("UPRN", 1), ("PropertyID", 2), ("Addr1", "hello")]),
            OrderedDict([("UPRN", 2), ("PropertyID", 3), ("Addr1", "Fred")]),
            OrderedDict([("UPRN", 3), ("PropertyID", 3), ("Addr1", "Beans")]),
        ]
        configure_db(db_file_path, self.db_fields, tables="test")
        write_to_db(data, db_file_path, self.db_fields, table="test")
        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute(
                """
                select * from test;
            """
            )
            rows = cursor.fetchall()
            for i, row in enumerate(rows):
                self.assertEqual([x for x in data[i].values()], list(row))

    def test_update_to_db(self):
        db_filename = "test_update_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test", force=True)
        write_to_db(data, db_file_path, self.db_fields, table="test")

        update_fields = ["Addr1", "UPRN"]
        update = [("Some", 3)]
        update_to_db(update, db_file_path, update_fields, table="test", key="UPRN")

        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute(
                """
                select Addr1 from test where UPRN = 3 ;
            """
            )
            rows = cursor.fetchall()
            expected = ("Some",)
            self.assertEqual(expected, rows[0])

    def test_update_to_db_compound_key(self):
        db_filename = "test_update_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test", force=True)
        write_to_db(data, db_file_path, self.db_fields, table="test")

        update_fields = ["Addr1", "UPRN", "PropertyID"]
        update = [("Some", 3, 3)]
        update_to_db(
            update,
            db_file_path,
            update_fields,
            table="test",
            key=["UPRN", "PropertyID"],
        )

        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute(
                """
                select Addr1 from test where UPRN = 3 ;
            """
            )
            rows = cursor.fetchall()
            expected = ("Some",)
            self.assertEqual(expected, rows[0])

    def test_update_dictionaries_to_db(self):
        db_filename = "test_update_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test", force=True)
        write_to_db(data, db_file_path, self.db_fields, table="test")

        update_fields = ["Addr1", "UPRN"]
        update = [OrderedDict([("Addr1", "Some"), ("UPRN", 3)])]

        update_to_db(update, db_file_path, update_fields, table="test", key="UPRN")

        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute(
                """
                select Addr1 from test where UPRN = 3 ;
            """
            )
            rows = cursor.fetchall()
            expected = ("Some",)
            self.assertEqual(expected, rows[0])

    def test_update_key_consistency_check(self):
        db_filename = "test_update_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test", force=True)
        write_to_db(data, db_file_path, self.db_fields, table="test")
        # This set of update_fields is deliberately wrong
        update_fields = ["PropertyID", "UPRN"]
        update = [OrderedDict([("Addr1", "Some"), ("UPRN", 3)])]

        try:
            self.assertRaises(
                update_to_db(update, db_file_path, update_fields, table="test", key="UPRN"),
                KeyError,
            )
        except KeyError:
            pass

    def test_update_to_db_no_nones(self):
        db_filename = "test_update_db2.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test", force=True)
        write_to_db(data, db_file_path, self.db_fields, table="test")

        update_fields = ["Addr1", "PropertyID", "UPRN"]
        update = [("Some", 1, 3), (None, 1, 2)]
        update_to_db(update, db_file_path, update_fields, table="test", key="UPRN")

        with sqlite3.connect(db_file_path) as c:
            cursor = c.cursor()
            cursor.execute(
                """
                select * from test;
            """
            )
            rows = cursor.fetchall()
            self.assertEqual(1, rows[1][1])
            self.assertEqual(1, rows[2][1])
            self.assertEqual("Fred", rows[1][2])

    def test_finalise_db(self):
        db_filename = "test_finalise_db.sqlite"
        db_file_path = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_file_path):
            os.remove(db_file_path)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_file_path, self.db_fields, tables="test")
        write_to_db(data, db_file_path, self.db_fields, table="test")
        finalise_db(db_file_path, index_name="idx_addr1", table="test", colname="Addr1")

    def test_read_db(self):
        db_filename = "test_finalise_db.sqlite"
        db_config = os.path.join(self.db_dir, db_filename)
        if os.path.isfile(db_config):
            os.remove(db_config)
        data = [(1, 2, "hello"), (2, 3, "Fred"), (3, 3, "Beans")]
        configure_db(db_config, self.db_fields, tables="test")
        write_to_db(data, db_config, self.db_fields, table="test")

        sql_query = "select * from test;"

        for i, row in enumerate(read_db(sql_query, db_config)):
            test_data = OrderedDict(zip(self.db_fields.keys(), data[i]))
            self.assertEqual(row, test_data)

    def test_read_db_doesnot_create_database(self):
        db_filename = "nonexistent_db.sqlite"
        db_config = os.path.join(self.db_dir, db_filename)

        if os.path.isfile(db_config):
            os.remove(db_config)

        sql_query = "select * from test;"

        try:
            results = list(read_db(sql_query, db_config))
        except IOError:
            pass

        self.assertEqual(os.path.isfile(db_config), False)
