#! /usr/bin/python

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="ubuntu"
__date__ ="$22-Nov-2009 21:01:36$"

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



class PathData():
    def __init__(self,db):
        " "
        self.db = db

    def create(self):
        ""
        self.db.execute('CREATE TABLE IF NOT EXISTS PathData (id INTEGER NOT NULL, Name TEXT, PRIMARY KEY (id))')

    def drop(self):
        self.db.execute('DROP TABLE IF EXISTS PathData')

    def exist(self,PathElement):
        ""
        result = False
        str = 'SELECT count(*) FROM PathData where Name="' + PathElement + '"'
        #
        rows = self.db.execute(str)
        for row in rows:
            if (row[0] != 0):
                result = True
        return result

    def insert(self,PathElement):
        ""
        self.db.execute('INSERT INTO PathData (Name) VALUES (?)', (PathElement,))


    def query(self,PathElement):
        ""
        ID = -1
        str = 'SELECT id FROM PathData where Name="' + PathElement + '"'
        rows = self.db.execute(str)
        for row in rows:
            ID = row[0]
        return ID

    def get(self,ID):
        ""
        cursor =  self.db.execute('SELECT * FROM PathData where id=?',(ID,))
        return cursor.fetchone()

    def getbyname(self,Name):
        ""
        cursor =  self.db.execute('SELECT * FROM PathData where Name=?',(Name,))
        return cursor.fetchone()


if __name__ == "__main__":
    print "Hello World";
