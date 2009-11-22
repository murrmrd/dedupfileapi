#! /usr/bin/python

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="ubuntu"
__date__ ="$22-Nov-2009 21:02:20$"

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


class Sequence():
    def __init__(self,db):
        " "
        self.db = db

    def create(self):
        ""
        self.db.execute('CREATE TABLE IF NOT EXISTS Sequence (id INTEGER NOT NULL, Sequence BLOB,PRIMARY KEY (id))')

    def drop(self):
        self.db.execute('DROP TABLE IF EXISTS Sequence')

    def query(self,SequenceDict):
        ""
        a = json.dumps(SequenceDict)
        compressed = zlib.compress(a)
        #original_data = a
        #print 'Original     :', len(original_data), original_data
        #compressed = zlib.compress(original_data)
        #print 'Compressed   :', len(compressed), binascii.hexlify(compressed)
        #decompressed = zlib.decompress(compressed)
        #print 'Decompressed :', len(decompressed), decompressed


        ID = -1
        #str = 'SELECT id FROM Sequence where sequence="' + sqlite3.Binary(compressed) + '"'
        #rows = self.db.execute(str)
        rows = self.db.execute('SELECT id FROM Sequence where sequence=?',(sqlite3.Binary(compressed),))
        for row in rows:
            ID = row[0]
        return ID

    def exist(self,SequenceDict):
        ""
        a = json.dumps(SequenceDict)
        compressed = zlib.compress(a)
        result = False
        #str = 'SELECT count(*) FROM Sequence where sequence="' + sqlite3.Binary(compressed) + '"'
        #
        #rows = self.db.execute(str)
        rows = self.db.execute('SELECT count(*) FROM Sequence where sequence=?',(sqlite3.Binary(compressed),))
        for row in rows:
            if (row[0] != 0):
                result = True
        return result

    def insert(self,SequenceDict):
        ""
        a = json.dumps(SequenceDict)
        compressed = zlib.compress(a)
        #print 'Compressed   :', len(a), len(compressed), binascii.hexlify(compressed)

        #self.db.execute('INSERT INTO Sequence (sequence) VALUES (?)', (sqlite3.Binary(SequenceDict),))
        #self.db.execute('INSERT INTO Sequence (sequence) VALUES (?)', (a,))
        self.db.execute('INSERT INTO Sequence (sequence) VALUES (?)', (sqlite3.Binary(compressed),))
        ID = -1
        rows = self.db.execute('SELECT id FROM Sequence where sequence=?',(sqlite3.Binary(compressed),))
        for row in rows:
            ID = row[0]
        return ID

    def get_sequence(self,ID):
        ""
        cursor =  self.db.execute('SELECT * FROM Sequence where id=?',(ID,))
        (id,compressed) = cursor.fetchone()
        decompressed = zlib.decompress(compressed)
        SequenceDict = json.loads(decompressed)
        return SequenceDict




if __name__ == "__main__":
    print "Hello World";
