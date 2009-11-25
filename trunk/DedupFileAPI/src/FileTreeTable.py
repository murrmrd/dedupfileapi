#! /usr/bin/python

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="ubuntu"
__date__ ="$22-Nov-2009 21:00:58$"

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


class FileTree():
    def __init__(self,db):
        " "
        self.db = db

    def create(self):
        ""
        self.db.execute('CREATE TABLE IF NOT EXISTS FileTree (id INTEGER NOT NULL, Parent INTEGER ,PathData_id INTEGER, Node_id INTEGER NOT NULL, PRIMARY KEY (id))')

    def drop(self):
        self.db.execute('DROP TABLE IF EXISTS FileTree')

    def exist(self, parent, PathDataID):
        ""
        str = 'SELECT count(id) FROM FileTree WHERE Parent=? AND PathData_id= ?'
        rows = self.db.execute(str, (parent, PathDataID, ))
        result = False
        for row in rows:
            if (row[0] != 0):
                result = True
        return result

    def insert(self,parent, PathDataID, NodeID):
        ""
        str = 'INSERT INTO FileTree (Parent, PathData_id, Node_id) VALUES (?,?,?)'
        self.db.execute(str, (parent, PathDataID,NodeID))

    def update(self,FileTreeID, NodeID):
        ""
        str='UPDATE FileTree SET Node_id = ? WHERE id = ?'
        self.db.execute(str, (NodeID,FileTreeID))

    def query(self,parent, PathDataID):
      ""
      str = 'SELECT id FROM FileTree WHERE Parent=? AND PathData_id= ?'
      rows = self.db.execute(str, (parent, PathDataID))
      result = -1
      for row in rows:
        result= row[0]
      return result

    def get(self,ID):
        ""
        cursor =  self.db.execute('SELECT * FROM FileTree where id=?',(ID,))
        return cursor.fetchone()

    def getbypathidandparent(self,PathID,ParentID):
        ""
        cursor =  self.db.execute('SELECT * FROM FileTree WHERE  AND Parent=?',(ID,))
        return cursor.fetchone()

    def getbynodeid(self,NodeID):
        ""
        cursor =  self.db.execute('SELECT * FROM FileTree where Node_id=?',(NodeID,))
        return cursor.fetchone()

    def getlistbyparent(self,TreeID):
        ""
        cursor =  self.db.execute('SELECT * FROM FileTree where Parent=?',(TreeID,))
        return cursor


if __name__ == "__main__":
    print "Hello World";
