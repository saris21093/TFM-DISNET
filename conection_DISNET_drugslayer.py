"""Conection to the DISNET drugslayer database.

This module needs the information of the database that the data will be stored.
"""
import mysql.connector
from mysql.connector import errorcode
DB_NAME = 'disnet_drugslayer'

cnx = mysql.connector.connect(host='138.4.130.153',
                            port= '30604'   ,
                            user='drugslayer',
                            password='drugs1234',
                            database = DB_NAME)
cursor = cnx.cursor()
cnx.autocommit = True

