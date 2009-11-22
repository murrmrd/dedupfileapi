import os.path
__author__ = "oliviero"
__date__ = "$23 oct. 2009 17:03:27$"

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


import DedupDB











ERROR_STR= """Error removing %(path)s, %(error)s """

def rmgeneric(path, __func__):
    try:
        __func__(path)
        print '* Removed ', path
    except OSError, (errno, strerror):
        print ERROR_STR % {'path' : path, 'error': strerror }

def removeall(path):
    ""
    if not os.path.isdir(path):
        return

    files=os.listdir(path)

    for x in files:
        fullpath=os.path.join(path, x)
        if os.path.isfile(fullpath):
            f=os.remove
            rmgeneric(fullpath, f)
        elif os.path.isdir(fullpath):
            removeall(fullpath)
            f=os.rmdir
            rmgeneric(fullpath, f)

def mkfile(FileName, Size):
    ""
    dir = os.path.dirname(FileName)

    if not os.path.exists(dir):
        os.makedirs(dir)
    fh = open(FileName, 'wb')
    for i in range (0,Size):
	fh.write(random.choice('abcdefghijklmonpqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ '))
    fh.close()
    print "* create ", FileName, "- Size = ",str(Size)

def comparefiles(file1,file2):
    ""
    # compare two files and check if they are equal
    # files can be binary or text based
    # pick two files you want to compare

    if filecmp.cmp(file1, file2):
        print "### Files %s and %s are identical" % (file1, file2)
    else:
        print "### Files %s and %s differ!" % (file1, file2)
        exit(-1)

def batch():
    print "Remove ",startDir
    removeall(startDir)

    print "Build some random files in ", startDir
    for i in range(1,2):
        mkfile(os.path.join(startDir,"filename"+str(i)+".txt"),i*500)
    for j in range(1,2):
        for i in range(1,2):
            NextDir = os.path.join(startDir,"subDIR"+str(j))
            print "Build some random files in ", NextDir
            mkfile(os.path.join(NextDir,"filename"+str(i)+".txt"),i*500)

    print "Prepare DB"
    db = DedupDB()

    print "Start archiving at ",startDir

    directories = [startDir]
    while len(directories)>0:
        directory = directories.pop()
        print "* Process directory: ",directory
        for name in os.listdir(directory):
            fullpath = os.path.join(directory,name)
            if os.path.isfile(fullpath):
                print "** Dedup file: ",fullpath
                db.archive(fullpath)

                print "** Archive it"
                (FullPath,FileName) = os.path.split(fullpath)
                (FilenameRoot,Extension) = os.path.splitext(FileName)
                newfile = fullpath.replace(Extension, '~'+Extension)

                if os.path.exists(newfile):
                    os.remove(newfile)
                os.rename(fullpath,newfile)

            elif os.path.isdir(fullpath):
                directories.append(fullpath)  # It's a directory, store it

        print "* Prepare to restore: ",directory
        TreeID = db.getfiletreeidbypath(directory)
        NodeIDList = db.listfiletree(TreeID,False)
        for NodeID in NodeIDList:
            restoredfilename = db.getfullpath(NodeID)
            (FullPath,FileName) = os.path.split(restoredfilename)
            (FilenameRoot,Extension) = os.path.splitext(FileName)
            backupfilename =  restoredfilename.replace(Extension, '~'+Extension)
            print "** Restore Node ",NodeID, " = ", restoredfilename
            db.restore(NodeID)
            comparefiles(backupfilename,restoredfilename)
    print "Done!"

def batch1():
    startDir = 'c:\\grafik'
    print os.getenv('HOME')
    startDir = os.path.join(os.getenv('HOME'),"DedupFileAPI-test")


    print "Remove ",startDir
    removeall(startDir)

    filename = os.path.join(startDir,'test.txt')
    mkfile(filename,500*1000)

    print "Prepare DB"
    db = DedupDB.DedupDB()

    print "** Dedup file: "
    db.archive(filename)

#    archive = zipfile.ZipFile('c:\\grafik\\test.zip', "w", zipfile.ZIP_DEFLATED) # "a" to append, "r" to read
#    archive.write('c:\\grafik\\test.txt')
#    archive.close()
#    print "** Archive original"
#    (FullPath,FileName) = os.path.split('c:\\grafik\\test.zip')
#    (FilenameRoot,Extension) = os.path.splitext(FileName)
#    newfile = 'c:\\grafik\\test.zip'.replace(Extension, '~'+Extension)
#    if os.path.exists(newfile):
#        os.remove(newfile)
#    os.rename('c:\\grafik\\test.zip',newfile)


    print "** Archive original"
    (FullPath,FileName) = os.path.split(filename)
    (FilenameRoot,Extension) = os.path.splitext(FileName)
    newfile = filename.replace(Extension, '~'+Extension)

    if os.path.exists(newfile):
        os.remove(newfile)
    os.rename(filename,newfile)

    TreeID = db.getfiletreeidbypath(startDir)
    NodeIDList = db.listfiletree(TreeID,False)
    for NodeID in NodeIDList:
            print NodeID
            restoredfilename = db.getfullpath(NodeID)
            print restoredfilename
            (FullPath,FileName) = os.path.split(restoredfilename)
            (FilenameRoot,Extension) = os.path.splitext(FileName)
            backupfilename =  restoredfilename.replace(Extension, '~'+Extension)
            print backupfilename
            print "** Restore Node ",NodeID, " = ", restoredfilename
            db.restore(NodeID)
            comparefiles(backupfilename,restoredfilename)

    stats = os.stat(filename)
    filesize =  stats[stat.ST_SIZE]
    stats = os.stat("database")
    databasesize = stats[stat.ST_SIZE]
    print filesize, databasesize, float(100*databasesize/filesize)



#    print "** Unzip"
#    zfobj = zipfile.ZipFile('c:\\grafik\\test.zip')
#    for name in zfobj.namelist():
#        if name.endswith('/'):
#            #os.mkdir(os.path.join(dir, name))
#            ""
#        else:
#            outfile = open(os.path.join('c:\\', name), 'wb')
#            outfile.write(zfobj.read(name))
#            outfile.close()

#    comparefiles('c:\\grafik\\test.txt',newfile)


if __name__ == "__main__":

    batch1()
    exit()