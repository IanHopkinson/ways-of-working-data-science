#!/usr/bin/env python
# encoding: utf-8
"""
This package contains functions relating to databases

Copied over from ninetales-data-visualisation on 2021-06-08

It originally came from ihutilities with the Elasticsearch elements removed

Ian Hopkinson
"""

import datetime
import os
import time
import sqlite3
import logging
import pymysql

from pymysql.constants.CR import CR_CONN_HOST_ERROR
from pymysql.constants.ER import BAD_DB_ERROR

from collections import OrderedDict

db_config_template = {
    "db_name": "test",
    "db_user": "root",
    "db_pw_environ": "MARIA_DB_PASSWORD",
    "db_host": "127.0.0.1",
    "db_conn": None,
    "db_type": "mysql",
    "db_path": None,
}

logger = logging.getLogger(__name__)


def configure_db(db_config, db_fields, tables="property_data", force=False):
    """This function sets up a sqlite or MariaDB/MySQL database

    Args:
        db_config (str or dict):
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
        db_fields (OrderedDict or dictionary of OrderedDicts):
            A dictionary of fieldnames and types per table

    Keyword Args:
        tables (string or list of strings):
            names of tables required, keys to db_fields
        force (bool):
            If using sqlite, force=True deletes existing files of db_config

    Returns
       db_config structure, in particular with the db_conn field populated for MariaDB/MySQL

    Example
        >>> db_fields = OrderedDict([
              ("UPRN","INTEGER PRIMARY KEY"),
              ("PropertyID", "INT"),
              ("Addr1", "TEXT"),
        ])
        >>> db_filename = "test_config_db.sqlite"
        >>> db_dir = "ihutilities\\fixtures"
        >>> db_file_path = os.path.join(db_dir, db_filename)
        >>> configure_db(db_file_path, db_fields, tables="test")
    """
    # Cunning polymorphism:
    # If we get a list and string then we convert them to a dictionary and a list
    # for backward compatibility

    db_config = _normalise_config(db_config)

    if isinstance(tables, str):
        tables = [tables]
        db_fields = {tables[0]: db_fields}
    # Convert old db_path string to db_config dictionary

    # Delete database if force is true
    if db_config["db_type"] == "sqlite":
        if os.path.isfile(db_config["db_path"]) and force:
            os.remove(db_config["db_path"])
        # If the directory doesn't exist then create it
        if not os.path.isdir(os.path.dirname(db_config["db_path"])):
            logger.warning(
                "Path to requested database ({}) does not exist, creating".format(
                    os.path.dirname(db_config["db_path"])
                )
            )
            os.makedirs(os.path.dirname(db_config["db_path"]))

        _ = _make_connection(db_config)
    # Default behaviour for mysql/mariadb is not to drop database
    elif db_config["db_type"] == "mysql" or db_config["db_type"] == "mariadb":
        # Make connection now wraps in creation of a new database if it doesn't exist
        _ = _make_connection(db_config)
        # cursor = conn.cursor()

    # Create tables, as specified
    _create_tables_db(db_config, db_fields, tables, force)
    # Close connection? or return db_config

    # db_config["db_conn"].commit()
    # db_config["db_conn"].close()

    return db_config


def write_to_db(data, db_config, db_fields, table="property_data", whatever=False):
    """
    This function writes a list of rows to a sqlite or MariaDB/MySQL database

    Args:
       data (list of lists or OrderedDicts):
            List of lists or OrderedDicts to write to database.
       db_config (str or dict):
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
       db_fields (OrderedDict or dictionary of OrderedDicts):
            A dictionary of fieldnames and types per table

    Keyword args:
       table (str):
            name of table to which we are writing, key to db_fields
       whatever (bool):
            If true each item is tried individually and only those accepted are written,
            list of those not inserted is returned

    Returns:
       No return value


    Notes:
        If data is prepared as a dictionary then it can be converted using:
        >>> ([x for x in new_row.values()])

    Example:
        >>> db_fields = OrderedDict([
              ("UPRN","INTEGER PRIMARY KEY"),
              ("PropertyID", "INT"),
              ("Addr1", "TEXT"),
        ])
        >>> db_filename = "test_write_db.sqlite"
        >>> db_dir = "ihutilities\\fixtures"
        >>> db_file_path = os.path.join(db_dir, db_filename)
        >>> data = [(1, 2, "hello"),
                    (2, 3, "Fred"),
                    (3, 3, "Beans")]
        >>> write_to_db(data, db_file_path, db_fields, table="test")
    """
    db_config = _normalise_config(db_config)

    ONE_PLACEHOLDER = ""
    if db_config["db_type"] == "sqlite":
        ONE_PLACEHOLDER = "?,"
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        ONE_PLACEHOLDER = "%s,"

    conn = _make_connection(db_config)
    cursor = conn.cursor()

    DB_INSERT_ROOT = "INSERT INTO {} (".format(table)
    DB_INSERT_MIDDLE = ") VALUES ("
    DB_INSERT_TAIL = ")"

    DB_FIELDS = DB_INSERT_ROOT
    DB_PLACEHOLDERS = DB_INSERT_MIDDLE

    for k in db_fields.keys():
        DB_FIELDS = DB_FIELDS + k + ","
        if db_fields[k] in [
            "POINT",
            "POLYGON",
            "LINESTRING",
            "MULTIPOLYGON",
            "GEOMETRY",
        ]:
            DB_PLACEHOLDERS = DB_PLACEHOLDERS + "GeomFromText(%s),"
        else:
            DB_PLACEHOLDERS = DB_PLACEHOLDERS + ONE_PLACEHOLDER

    INSERT_statement = DB_FIELDS[0:-1] + DB_PLACEHOLDERS[0:-1] + DB_INSERT_TAIL

    rejected_data = []

    # convert a list of dictionary to a list of lists, if required:

    converted_data = []
    if len(data) > 0 and isinstance(data[0], dict):
        for row in data:
            converted_data.append([x for x in row.values()])
    else:
        converted_data = data

    if whatever:
        for row in converted_data:
            try:
                cursor.execute(INSERT_statement, row)
            except (pymysql.err.IntegrityError, sqlite3.IntegrityError):
                rejected_data.append(row)

    else:
        try:
            logger.debug(
                "Insert statement = {}\nData line 1 = {}".format(
                    INSERT_statement, converted_data[0]
                )
            )
            cursor.executemany(INSERT_statement, converted_data)
        except (pymysql.err.IntegrityError, sqlite3.IntegrityError):
            conn.close()
            raise
        except (pymysql.err.DataError):
            conn.close()
            logger.info("write_to_db failed with {}".format(converted_data))
            raise

    conn.commit()
    conn.close()

    return rejected_data


def update_to_db(data, db_config, db_fields, table="property_data", key=["UPRN"]):
    """
    This function updates rows in a sqlite or MariaDB/MySQL database

    Args:
       data (list of lists or dictionaries):
            List of lists of data to update to database, order matches db_fields
       db_config (str or dict):
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template
       db_fields (OrderedDict):
            A list of fieldnames in an OrderedDict containing the fields to update and
            the key field in the order in which the fields are presented in the data lists

    Keyword args:
       table (str):
            name of table to which we are writing, key to db_fields
       key (str):
            the field which forms the key of the update

    Returns:
       No return value

    Example:
        >>> db_fields = OrderedDict([
              ("UPRN","INTEGER PRIMARY KEY"),
              ("PropertyID", "INT"),
              ("Addr1", "TEXT"),
        ])
        >>> db_filename = "test_write_db.sqlite"
        >>> db_dir = "ihutilities\\fixtures"
        >>> db_file_path = os.path.join(db_dir, db_filename)
        >>> data = [(1, 2, "hello"),
                    (2, 3, "Fred"),
                    (3, 3, "Beans")]
        >>> update_fields = ["Addr1", "UPRN"]
        >>> update = [("Some", 3)]
        >>> update_to_db(update, db_file_path, update_fields, table="test", key="UPRN")
    """
    if isinstance(key, str):
        key = [key]

    db_config = _normalise_config(db_config)

    PLACEHOLDER = ""
    DB_UPDATE_TAIL = ""
    if db_config["db_type"] == "sqlite":
        # WHERE KEY1 = ?
        # WHERE KEY1 = ? AND KEY2 = ?
        # WHERE KEY1 = ? AND KEY2 = ? AND KEY3 = ?
        DB_UPDATE_TAIL = " WHERE "
        joiner = ""
        for k in key:
            DB_UPDATE_TAIL = DB_UPDATE_TAIL + "{} {} = ?".format(joiner, k)
            joiner = "AND"
        PLACEHOLDER = " = ?,"
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        DB_UPDATE_TAIL = " WHERE "
        joiner = ""
        for k in key:
            DB_UPDATE_TAIL = DB_UPDATE_TAIL + "{} {} = %s".format(joiner, k)
            joiner = "AND"
        PLACEHOLDER = " = %s,"

    conn = _make_connection(db_config)
    cursor = conn.cursor()
    DB_UPDATE_ROOT = "UPDATE {} SET ".format(table)

    key_indices = []
    for k in key:
        key_index = db_fields.index(k)
        key_indices.append(key_index)

    # convert a list of dictionary to a list of lists, if required:

    converted_data = []
    if isinstance(data[0], dict):
        for row in data:
            converted_data.append([x for x in row.values()])

        if db_fields != list(data[0].keys()):
            raise KeyError(
                f"db_fields supplied to update_to_db ('{db_fields}')"
                f"do not match fields in update dictionary {list(data[0].keys())}"
            )
    else:
        converted_data = data
    for row in converted_data:
        # print(update_statement, [x for x in row])
        key_vals = []
        for k in key_indices:
            key_vals.append(row[k])
        update_fields = []
        update_data = []
        for i, _ in enumerate(row):
            if i not in key_indices and row[i] is not None:
                update_fields.append(db_fields[i])
                update_data.append(row[i])

        update_statement = ""
        DB_FIELDS = DB_UPDATE_ROOT
        for k in update_fields:
            DB_FIELDS = DB_FIELDS + k + PLACEHOLDER
            update_statement = DB_FIELDS[0:-1] + DB_UPDATE_TAIL

        update_data.extend(key_vals)
        if len(update_fields) != 0:
            logging.debug(
                "Attempting update with statement = '{}' and data = '{}'".format(
                    update_statement, update_data
                )
            )
            cursor.execute(update_statement, update_data)

    conn.commit()
    conn.close()


def drop_db_tables(db_config, tables):
    db_config = _normalise_config(db_config)
    conn = _make_connection(db_config)
    cursor = conn.cursor()

    for table in tables:
        cursor.execute("DROP TABLE IF EXISTS {}".format(table))
    conn.close()


def finalise_db(
    db_config,
    index_name="idx_postcode",
    table="property_data",
    colname="postcode",
    spatial=False,
):
    """
    This function creates an index in a sqlite or MariaDB/MySQL database

    Args:
       db_config (str or dict):
            For sqlite a file path in a string is sufficient, MariaDB/MySQL require
            a dictionary and example of which is found in db_config_template

    Keyword args:
       index_name (str):
            name of the index to be created
       table (str):
            the table on which the index is to be created
       colname (str):
            the column on which the index is to be created
       spatial (bool):
            True for a spatial index, false otherwise

    Returns:
       No return value

    """

    db_config = _normalise_config(db_config)

    conn = _make_connection(db_config)
    cursor = conn.cursor()

    if isinstance(colname, list):
        colname = ",".join(colname)

    time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(
        "Creating index named '{}' on column(s) '{}' at {}".format(index_name, colname, time_str)
    )
    if spatial:
        cursor.execute(
            "CREATE SPATIAL INDEX {index_name} on {table}({colname})".format(
                index_name=index_name, table=table, colname=colname
            )
        )
    else:
        cursor.execute(
            "CREATE INDEX {index_name} on {table}({colname} ASC)".format(
                index_name=index_name, table=table, colname=colname
            )
        )
    conn.commit()
    conn.close()


def read_db(sql_query, db_config):
    db_config = _normalise_config(db_config)

    err_wait = 30.0

    if db_config["db_type"] == "sqlite" and not os.path.isfile(db_config["db_path"]):
        raise IOError("Database file '{}' does not exist".format(db_config["db_path"]))

    try:
        conn = _make_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(sql_query)
    except pymysql.Error as err:
        if err.args[0] == CR_CONN_HOST_ERROR:
            logger.warning(
                f"Caught exception '{err}'. errno = '{err.args[0]}', "
                f"waiting {err_wait} seconds and having another go"
            )
            time.sleep(err_wait)
            conn = _make_connection(db_config)
            cursor = conn.cursor()
            cursor.execute(sql_query)
        else:
            raise
    except sqlite3.OperationalError as err:
        logger.info("Caught exception {} on query '{}'".format(err, sql_query))
        print("Caught exception {} on query '{}'".format(err, sql_query), flush=True)
        raise

    colnames = [x[0] for x in cursor.description]

    # rows = cursor.fetchall()
    # conn.close()

    # for row in rows:
    #     if row is not None:
    #         labelled_row = OrderedDict(zip(colnames, row))
    #         yield labelled_row

    while True:
        row = cursor.fetchone()
        if row is not None:
            labelled_row = OrderedDict(zip(colnames, row))
            yield labelled_row
        else:
            conn.close()
            # raise StopIteration # - this is depreciated in Python 3.5 onwards
            return


def delete_from_db(sql_query, db_config):
    db_config = _normalise_config(db_config)

    err_wait = 30.0

    if db_config["db_type"] == "sqlite" and not os.path.isfile(db_config["db_path"]):
        raise IOError("Database file '{}' does not exist".format(db_config["db_path"]))

    try:
        conn = _make_connection(db_config)
        cursor = conn.cursor()
        cursor.execute(sql_query)
    except pymysql.Error as err:
        if err.args[0] == CR_CONN_HOST_ERROR:
            logger.warning(
                f"Caught exception '{err}'. errno = '{err.errno}', "
                f"waiting {err_wait} seconds and having another go"
            )
            time.sleep(err_wait)
            conn = _make_connection(db_config)
            cursor = conn.cursor()
            cursor.execute(sql_query)
        else:
            raise
    except sqlite3.OperationalError as err:
        logger.info("Caught exception {} on query '{}'".format(err, sql_query))
        print("Caught exception {} on query '{}'".format(err, sql_query), flush=True)
        raise

    if conn:
        conn.commit()
        conn.close()


def _normalise_config(db_config):
    """
    This is a private function which will expand a db_config string into
    the dictionary format.
    """

    if isinstance(db_config, str):
        db_path = db_config
        db_config = db_config_template.copy()
        db_config["db_type"] = "sqlite"
        db_config["db_path"] = db_path
    return db_config


def _make_connection(db_config):
    """
    This is a private function responsible for making a connection to the database
    """

    if db_config["db_type"] == "sqlite":
        db_config["db_conn"] = sqlite3.connect(db_config["db_path"])
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        if not check_mysql_database_exists(db_config):
            create_mysql_database(db_config)

        # This code much fiddled with, essentially I was trying to do my own connection pooling
        # on top of the connectors pooling and it didn't work.
        # I was getting pool exhaustion because I wasn't closing connections,
        # this should now be fixed (fingers crossed)
        # if db_config["db_conn"] is None or True:
        password = os.environ[db_config["db_pw_environ"]]
        conn = pymysql.connect(
            database=db_config["db_name"],
            user=db_config["db_user"],
            password=password,
            host=db_config["db_host"],
        )
        # pool_name=db_config["db_name"],
        # pool_size=10)
        db_config["db_conn"] = conn
        # else:
        #    print("Returning old connection", flush=True)
        #    conn = db_config["db_conn"]

        # Bit messy, sometimes we make a connection without db existing
        try:
            conn.database = db_config["db_name"]
        except pymysql.Error as err:
            if err.args[0] != BAD_DB_ERROR:
                raise

    return db_config["db_conn"]


def create_mysql_database(db_config):
    password = os.environ[db_config["db_pw_environ"]]
    conn = pymysql.connect(user=db_config["db_user"], password=password, host=db_config["db_host"])
    cursor = conn.cursor()
    create_string = (
        "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci'".format(
            db_config["db_name"]
        )
    )
    try:
        cursor.execute(create_string)
    except pymysql.Error as err:
        logger.critical("Failed creating database: {}".format(err))
        logger.critical("Creation command: {}".format(create_string))
        exit(1)

    conn.commit()
    conn.close()


def check_mysql_database_exists(db_config):
    sql_query = (
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{}'".format(
            db_config["db_name"]
        )
    )
    password = os.environ[db_config["db_pw_environ"]]
    conn = pymysql.connect(user=db_config["db_user"], password=password, host=db_config["db_host"])
    cursor = conn.cursor()
    cursor.execute(sql_query)
    # conn.commit()

    results = cursor.fetchall()
    if results is not None and len(results) == 1:
        exists = True
    elif results is not None and len(results) == 0:
        exists = False
    else:
        raise ValueError("Found multiple databases with the same name")

    conn.commit()
    conn.close()
    return exists


def check_table_exists(db_config, table):
    db_config = _normalise_config(db_config)
    conn = _make_connection(db_config)

    if db_config["db_type"] == "sqlite":
        table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='{}';"
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        table_check_query = (
            "SELECT table_name as name FROM information_schema.tables "
            "WHERE table_schema = '{}'".format(db_config["db_name"]) + " AND table_name = '{}';"
        )

    cursor = conn.cursor()

    cursor.execute(table_check_query.format(table))
    result = cursor.fetchall()
    logger.debug("table_check_query result: {}".format(result))
    if len(result) != 0 and result[0][0].lower() == table.lower():
        table_exists = True
    else:
        table_exists = False

    return table_exists


def _create_tables_db(db_config, db_fields, tables, force):
    """
    This is a private function responsible for creating a database table
    """
    if db_config["db_type"] == "sqlite":
        table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='{}';"
        DB_CREATE_TAIL = ")"
        name = os.path.basename(db_config["db_path"])
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        table_check_query = (
            "SELECT table_name as name FROM information_schema.tables "
            "WHERE table_schema = '{}'".format(db_config["db_name"]) + " AND table_name = '{}';"
        )
        DB_CREATE_TAIL = ") ENGINE = MyISAM"
        name = db_config["db_name"]

    conn = db_config["db_conn"]
    cursor = conn.cursor()
    for table in tables:
        DB_CREATE_ROOT = "CREATE TABLE {} (".format(table)

        DB_CREATE = DB_CREATE_ROOT
        primary_keys = []
        for k, v in db_fields[table].items():
            if (
                db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql"
            ) and "AUTOINCREMENT" in v:
                v = v.replace("AUTOINCREMENT", "AUTO_INCREMENT")

            if (
                "PRIMARY KEY" in v.upper()
                and ("AUTO_INCREMENT" not in v)
                and ("AUTOINCREMENT" not in v)
            ):
                v = v.replace("PRIMARY KEY", "")
                primary_keys.append(k)

            if v in ["POINT", "POLYGON", "LINESTRING", "MULTIPOLYGON", "GEOMETRY"]:
                logger.debug(
                    f"Appending NOT NULL to {v} in {table}"
                    "to allow spatial indexing in MariaDB/MySQL [_create_tables_db]"
                )
                DB_CREATE = DB_CREATE + " ".join([k, v]) + " NOT NULL,"
            else:
                DB_CREATE = DB_CREATE + " ".join([k, v]) + ","

        # add in the PRIMARY KEY clause
        if len(primary_keys) == 0:
            logger.warning("No primary keys supplied for table '{}'".format(table))
            DB_CREATE = DB_CREATE[0:-1] + DB_CREATE_TAIL
        else:
            PRIMARY_KEY_CLAUSE = "PRIMARY KEY ({})".format(",".join(primary_keys))
            # Since we're using a separate primary key clause we don't need to clip a trailing comma
            DB_CREATE = DB_CREATE + PRIMARY_KEY_CLAUSE
            DB_CREATE = DB_CREATE + DB_CREATE_TAIL

        if force and db_config["db_type"] == "sqlite":
            cursor.execute("DROP TABLE IF EXISTS {}".format(table))
            logger.warning(
                "Force is True, so dropping table '{}' in database '{}'".format(table, name)
            )
        elif force and (db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql"):
            cursor.execute("DROP TABLE IF EXISTS `{}`.`{}`;".format(db_config["db_name"], table))
            logger.warning(
                "Force is True, so dropping table '{}' in database '{}'".format(table, name)
            )

        cursor.execute(table_check_query.format(table))
        result = cursor.fetchall()
        logger.debug("table_check_query result: {}".format(result))
        if len(result) != 0 and result[0][0].lower() == table.lower():
            table_exists = True
        else:
            table_exists = False

        if not table_exists:
            logger.info("Creating table {} with statement: \n{}".format(table, DB_CREATE))
            try:
                cursor.execute(DB_CREATE)
            except:  # noqa: E722
                logger.debug(
                    "Database create statement failed: '{}' for database '{}'".format(
                        DB_CREATE, name
                    )
                )
                raise
        else:
            logger.warning("Table '{}' already exists in database '{}'".format(table, name))

    db_config["db_conn"].commit()
    db_config["db_conn"].close()


def list_tables(db_config):
    db_config = _normalise_config(db_config)
    table_check_query = ""
    if db_config["db_type"] == "sqlite":
        table_check_query = "SELECT name FROM sqlite_master WHERE type='table';"
    elif db_config["db_type"] == "mariadb" or db_config["db_type"] == "mysql":
        table_check_query = (
            "SELECT table_name as name FROM "
            "information_schema.tables WHERE table_schema = '{}';".format(db_config["db_name"])
        )

    conn = _make_connection(db_config)
    cursor = conn.cursor()

    cursor.execute(table_check_query)

    result = cursor.fetchall()
    return result
