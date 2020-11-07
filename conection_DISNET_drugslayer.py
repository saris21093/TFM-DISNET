"""Conection to the DISNET drugslayer database.

This module needs the information of the database that the data will be stored.
"""
import mysql.connector
from mysql.connector import errorcode
DB_NAME = ''

cnx = mysql.connector.connect(host='',
                            port= ''   ,
                            user='',
                            password='',
                            database = DB_NAME)
cursor = cnx.cursor()
cnx.autocommit = True

