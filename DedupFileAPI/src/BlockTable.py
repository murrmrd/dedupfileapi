#! /usr/bin/python

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="ubuntu"
__date__ ="$22-Nov-2009 20:59:49$"

import zlib
import binascii
import sqlite3
import sys
import time
import hashlib
import logging
import os
import stat
import tempfile
import zipfile
import json
import collections
import filecmp
import random


class Block():
    def __init__(self,db):
        " "
        self.db = db

    def create(self):
        "sqllite automatically create indexes for primary key and 'unique' colums"
        #self.db.execute('CREATE TABLE IF NOT EXISTS DataBlocks (id INTEGER NOT NULL,HashKey TEXT, Data BLOB,PRIMARY KEY (id))')
        self.db.execute('CREATE TABLE IF NOT EXISTS DataBlocks (id INTEGER NOT NULL, Data BLOB UNIQUE,PRIMARY KEY (id))')


    def drop(self):
        self.db.execute('DROP TABLE IF EXISTS DataBlocks')

    def get(self,ID):
        ""
        cursor =  self.db.execute('SELECT * FROM DataBlocks where id=?',(ID,))
        return cursor.fetchone()

#    def exist(self,HashKey):
#        ""
#        rows=self.db.execute('SELECT id FROM DataBlocks where HashKey=?',(HashKey,))
#        result = False
#        for row in rows:
#            if (row[0] != 0):
#                result = True
#        return result

    def exist_block(self,data):
        ""
        rows=self.db.execute('SELECT id FROM DataBlocks where Data=?',(sqlite3.Binary(data),))
        result = False
        for row in rows:
            if (row[0] != 0):
                result = True
        return result

    def insert(self,data):
        ""
        #self.db.execute('INSERT INTO DataBlocks (HashKey, Data) VALUES (?, ?)', (hashlib.sha512(data).hexdigest(), sqlite3.Binary(data)))
        self.db.execute('INSERT INTO DataBlocks (Data) VALUES (?)', (sqlite3.Binary(data),))

#    def query(self,HashKey):
#        ""
#        ID = -1
#        rows = self.db.execute('SELECT id FROM DataBlocks where HashKey=?',(HashKey,))
#        for row in rows:
#            ID = row[0]
#        return ID

    def query_block(self,data):
        ""
        ID = -1
        rows = self.db.execute('SELECT id FROM DataBlocks where Data=?',(sqlite3.Binary(data),))
        for row in rows:
            ID = row[0]
        return ID

if __name__ == "__main__":
    print "Hello World";
