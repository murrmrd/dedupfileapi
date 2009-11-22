#! /usr/bin/python

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="ubuntu"
__date__ ="$22-Nov-2009 21:00:18$"

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



class Node():
    def __init__(self,db):
        " "
        self.db = db

    def create(self):
        ""
        self.db.execute('CREATE TABLE IF NOT EXISTS Node (id INTEGER NOT NULL, BlockSequence_id INTEGER,Extension_id INTEGER, size  INTEGER, createtime INTEGER, lastmodifiddtime INTEGER, lastaccessedtime INTEGER, compressedflag BOOL, PRIMARY KEY (id))')

    def drop(self):
        self.db.execute('DROP TABLE IF EXISTS Node')

    def exist(self,ID):
        ""
        result = False
        str = 'SELECT count(*) FROM Node where PathData_id=?'
        #
        rows = self.db.execute(str,(ID,))
        for row in rows:
            if (row[0] != 0):
                result = True
        return result

    def insert(self,sequence,extension,size, createtime, lastaccessedtime, lastmodifiedtime,compressedflag):
        ""
        self.db.execute('INSERT INTO Node (BlockSequence_id,Extension_id,size, createtime, lastmodifiddtime, lastaccessedtime,compressedflag) VALUES (?,?,?,?,?,?,?)', (sequence,extension,size, createtime, lastmodifiedtime, lastaccessedtime,compressedflag,))

    def query(self,sequence):
        ""
        ID = -1
        rows = self.db.execute('SELECT id FROM Node where BlockSequence_id=?',(sequence,))
        for row in rows:
            ID = row[0]
        return ID

    def get(self,NodeID):
        ""
        cursor =  self.db.execute('SELECT * FROM Node where id=?',(NodeID,))
        return cursor.fetchone()


if __name__ == "__main__":
    print "Hello World";
