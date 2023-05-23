"""
Library to make connecting to a NREMC database and running commands simpler

Classes:
    NREMCDatabaseConnector
    
Misc Types:
    Commands
"""

import pyodbc
import tomli
from pyodbc import Cursor, Row
from typing import Any, Optional, Literal

Commands = dict[str, str]


class NREMCDatabaseConnector(object):
    """Class that creates and maintains a database connection with pyodbc.

    Creates a server connection using pyodbc to allow the user to execute stored
    commands and retrieve information, insert data, update data, or delete data

    Attributes:
        _conn (Connection): Pyodbc connection to the sql database
        _crsr (Cursor): Pyodbc cursor to execute sql code on the server
        _cmds (Commands): Dictionary of command identifiers to predefined sql commands
    """

    def __init__(
        self,
        server: str = ".",
        database: str = "master",
        version: int = 17,
        cmds: Commands = {},
    ) -> None:
        """Creates a connection to the sql server

        Takes in the servers name which database to connect too and the ODBC driver version to use
        Additionally takes in a dict with all command identifiers and predefined commands to execute

        Args:
            server (str, optional): Name of server to connect to. Defaults to ".".
            database (str, optional): Name of database to connect to. Defaults to "master".
            version (int, optional): Pyodbc driver version to use. Defaults to 17.
            cmds (Commands, optional): Dictionary of commands to use. Defaults to {}.
        """
        conn_str = (
            f"Driver={{ODBC DRIVER {version} for SQL Server}};"
            f"SERVER={server};DATABASE={database};ENCRYPT=no;"
            f"Trusted_Connection=yes"
        )
        self._conn = pyodbc.connect(conn_str)
        self._crsr = self._conn.cursor()
        self._crsr.fast_executemany = True
        self._cmds = cmds

    def __del__(self):
        """Destructor to close all connections"""
        self._crsr.close()
        self._conn.close()

    @staticmethod
    def from_toml_config(
        f_loc: str,
        server_header: str = "sql_server",
        command_header: str = "sql_commands",
    ):
        """Static method to create a Connection using a toml config file.

        Args:
            f_loc (str): Location of the toml file to load in
            server_header (str, optional): Toml config header to find info on connecting to the sql server. Defaults to "sql_server".
            command_header (str, optional): Toml config header to find info on sql commands to store. Defaults to "sql_commands".

        Returns:
            NREMCDatabaseConnector (NREMCDatabaseConnector): Live connection to database defined in the config file
        """
        with open(f_loc, "rb") as f:
            config = tomli.load(f)

        server_config = config[server_header]

        server = server_config["server"]
        database = server_config["database"]
        version = server_config["version"]

        command_config = config[command_header]
        return NREMCDatabaseConnector(server, database, version, command_config)

    @property
    def cursor(self) -> Cursor:
        """Cursor: Returns a cursor connected to the SQL server"""
        return self._crsr

    def call(self, cmd: str, *args) -> Cursor:
        """Executes a predefined sql command

        Takes in a command identifier and then passes all args to that command
        to execute the command and then returns the cursor

        Args:
            cmd (str): Command identifier to tell the connection which command to run
            *args (tuple[Any]): Arguments to pass to the command

        Returns:
            Cursor: Returns a cursor connection to the server after the command is executed
        """
        return self._crsr.execute(self._cmds[cmd], *args)

    def call_many(self, cmd: str, *args) -> None:
        """Executes a predefined sql command many times.

        Args:
            cmd (str): Command identifier to tell the connection which command to run.
            *args (tuple[Any]): Arguments to pass to the command.
        """
        self._crsr.executemany(self._cmds[cmd], *args)

    def fill_insert(self, cmd: str, keys_and_values: dict[Any, Any]) -> Cursor:
        """Fills a insert command by taking the keys from 'keys_and_values' and setting them as the columns and the values from 'keys_and_values' and executing the query with those values
        
        Args:
            cmd (str): Command identifier to tell the connection which command to run
            keys_and_values (dict[Any, Any]): Dict of the columns you want to insert into and values to place into those columns

        Returns:
            Cursor: Returns a cursor connection to the server after the command is executed
        """
        query = self._cmds[cmd].format(
            ", ".join(keys_and_values.keys()),
            ", ".join(["?"] * len(keys_and_values))
        )
        values = tuple(keys_and_values.values())
        return self._crsr.execute(query, values)
        
    def fill_update(self, cmd: str, keys_and_values: dict[Any, Any], conditional_keys: list[Any], conditional_connectors: Optional[list[Literal["AND", "OR"]]] = None) -> Cursor:
        """Fills a update command by using a dict and taking its keys as columns to update and search by and its values as new values or search values

        Args:
            cmd (str): Command identifier to tell the connection which command to run
            keys_and_values (dict[Any, Any]): Dict of the columns you wan to update and or search and values associated with  those columns
            conditional_keys (list[Any]): Keys in the dict that are meant to go in the where close
            conditional_connectors (Optional[list[Literal[&quot;AND&quot;, &quot;OR&quot;]]], optional): If two conditional keys are given they need a connector either 'AND' or 'OR'. Defaults to None.

        Returns:
            Cursor: Returns a cursor connection to the server after the command is executed
        """
        update_query = self._cmds[cmd]
        vals = []
        
        for key, val in keys_and_values.items():
            if key not in conditional_keys:
                update_query += f"{key} = ?, "
                vals.append(val)
        
        cond_vals = []
        
        conds = list(zip(conditional_keys, conditional_connectors + [""]))
        update_query = update_query.rstrip(", ") + "WHERE = "
        for cond, connector in conds:
            update_query += cond + " = ?" + connector + ", "
            cond_vals.append(keys_and_values[cond])
        update_query = update_query.rstrip(", ")
        
        return self._crsr.execute(update_query, vals.extend(cond_vals))
        

    def fetch(self, size: int = 1) -> Row | list[Row] | None:
        """Fetches the results from a SELECT query on the database

        Args:
            size (int, optional): How many row you want to fetch from the cursor. Defaults to 1.

        Returns:
            Row | list[Row] | None: Either returns one row a list of Rows or none is size is less than one
        """
        if size == 1:
            return self._crsr.fetchone()
        elif size > 1:
            return self._crsr.fetchmany(size)
        return None

    def fetch_all(self) -> list[Row]:
        """Fetches all queruered rows from a SQL command

        Returns:
            list[Row]: All row retrieved by SQL command
        """
        return self._crsr.fetchall()

    def commit(self) -> None:
        """Commits any changes to the database making them perfect"""
        self._crsr.commit()

    def rollback(self) -> None:
        """Rolls back any non committed changes to the database"""
        self._crsr.rollback()

    def set_command(self, identifier: str, command: str) -> None:
        """Sets a command by either creating a new command or updating a command

        Args:
            identifier (str): Command identifier for the command
            command (str): SQL command to run on the SQL server
        """
        self._cmds[identifier] = command
