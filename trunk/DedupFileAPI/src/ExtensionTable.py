#! /usr/bin/python

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="ubuntu"
__date__ ="$22-Nov-2009 20:57:09$"

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



class Extension():
    def __init__(self,db):
        " "
        self.db = db

    def create(self):
        ""
        self.db.execute('CREATE TABLE IF NOT EXISTS Extension (id INTEGER NOT NULL, Name TEXT , PRIMARY KEY (id))')

    def drop(self):
        self.db.execute('DROP TABLE IF EXISTS Extension')



    def exist(self,name):
        ""
        result = False
        str = 'SELECT count(*) FROM Extension where Name=?'
        #
        rows = self.db.execute(str,(name,))
        for row in rows:
            if (row[0] != 0):
                result = True
        return result

    def insert(self,name):
        ""
        self.db.execute('INSERT INTO Extension (name) VALUES (?)', (name,))

    def query(self,name):
        ""
        ID = -1
        rows = self.db.execute('SELECT id FROM Extension where name=?',(name,))
        for row in rows:
            ID = row[0]
        return ID

    def get(self,NodeID):
        ""
        cursor =  self.db.execute('SELECT * FROM Extension where id=?',(NodeID,))
        return cursor.fetchone()


if __name__ == "__main__":
    print "Hello World";
