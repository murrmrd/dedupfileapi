__author__ = "oliviero"
__date__ = "$23 oct. 2009 17:03:27$"

import os
import stat
import time
import hashlib #requises python2.6
import zipfile
import tempfile
import logging
# Ensure that we're running Python 3 or later.
import sys
# sqlite3 became part of the standard library as of Python 2.5.
import sqlite3


class DedupDB ():
    logger = logging.getLogger("DedupDB")
    def __init__(self):
        " "
        # create logger

        self.logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        self.logger.addHandler(ch)

        self.logger.debug("Connect to db")
        self.db = sqlite3.connect('database')

        self.logger.debug("Drop tables")
        self.db.execute('DROP TABLE IF EXISTS PathData')
        self.db.execute('DROP TABLE IF EXISTS FileTree')
        self.db.execute('DROP TABLE IF EXISTS Node')
        self.db.execute('DROP TABLE IF EXISTS DataBlocks')
        self.db.execute('DROP TABLE IF EXISTS demo')


        self.logger.debug("Create tables")
        self.db.execute('CREATE TABLE IF NOT EXISTS PathData (id INTEGER NOT NULL,HashKey TEXT, Name TEXT,PRIMARY KEY (id))')
        self.db.execute('CREATE TABLE IF NOT EXISTS FileTree (id INTEGER NOT NULL, Parent INTEGER, Node_id INTEGER NOT NULL, PRIMARY KEY (id))')
        self.db.execute('CREATE TABLE IF NOT EXISTS Node (id INTEGER NOT NULL, BlockSequence_id INTEGER, PathData_id INTEGER NOT NULL, PRIMARY KEY (id))')
        self.db.execute('CREATE TABLE IF NOT EXISTS DataBlocks (id INTEGER NOT NULL,HashKey TEXT, Data BLOB,PRIMARY KEY (id))')

    def archive(self, dedupfile):
        self.logger.debug("# Prepare path for Deduplication")
        # process path to build a dictionnary for path
        # exemple: c:\grafik => {'hash("c:"):"c:"','hash("grafik"):"grafik"'}
        __FullPath__ = dedupfile.getFullPath()
        __PathSeparator__ = dedupfile.getPathSeparator()
        PathElements = __FullPath__.split(__PathSeparator__)
        for PathElement in PathElements:
            HashKey = hashlib.sha512(PathElement).hexdigest()
            str = 'SELECT count(*) FROM PathData where HashKey="'+HashKey+'"'

            # if doesn't exist than insert
            rows = self.db.execute(str)
            for row in rows:
                if (row[0]==0):
                    self.db.execute('INSERT INTO PathData (HashKey, Name) VALUES (?, ?)',(HashKey,PathElement))

            # collect "id" in "PathData" table's row where "Name" = PathElement
            str = 'SELECT id FROM PathData where HashKey="'+HashKey+'"'
            rows = self.db.execute(str)
            for row in rows:
                ID = row[0]
                # ID is "id" in "PathData" table's row where "Name" = PathElement
                print "ID=",ID,PathElement

            # print out "PathData" table
            #rows = self.db.execute('SELECT * FROM PathData')
            #for row in rows:
            #    print "PathData +",row[0],"+",row[1],"+",row[2]

            str = 'SELECT id FROM Node where PathData_id=?'
            rows = self.db.execute(str,(ID,))
            NodeID = -1
            for row in rows:
                NodeID = row[0]
            print "NodeID=",NodeID
            if (NodeID==-1):
                print "Create new Node"
                str = 'INSERT INTO Node (BlockSequence_id, PathData_id) VALUES (?,?)'
                rows = self.db.execute(str,(-1,ID))
                str = 'SELECT id FROM Node where PathData_id=?'
                # collect newly create row id
                rows = self.db.execute(str,(ID,))
                for row in rows:
                    NodeID = row[0]
                print "newly created NodeID=",NodeID
            else:
                print "the node already exist"
                                 
            # 
            str = 'SELECT id FROM FileTree where Node_id=?'
            rows = self.db.execute(str,(ID,))
            PathEntryID = -1
            for row in rows:
                PathEntryID = row[0]
            print "PathEntryID=",PathEntryID
            # 
            if PathEntryID==-1:
                print "Create new PathEntry"
                str = 'INSERT INTO FileTree (Parent, Node_id) VALUES (?,?)'
                rows = self.db.execute(str,(-1,NodeID))
                str = 'SELECT id FROM FileTree where Node_id=?'
                # collect newly create row id
                rows = self.db.execute(str,(ID,))
                for row in rows:
                    PathEntryID = row[0]
                print "newly created PathEntryID=",PathEntryID
            else:
                print "the PathEntry already exist"


            #print out "FileTree" table
            #    rows = self.db.execute('SELECT * FROM FileTree')
            #    for row in rows:
            #        print  "FileTree +",row[0],"+",row[1],"+",row[2]



            




        # debug
        rows = self.db.execute('SELECT * FROM PathData')
        #for id, HashKey, Name in rows:
        #    print type(id), id
        #    print type(HashKey), HashKey
        #    print type(Name), Name

        self.logger.debug("# Split temporary compressed file into blocks")
        TempFilename = dedupfile.getTempFilename()
        FileAccelerator=hashlib.sha512()
        BlockSize=dedupfile.getBlockSize()

        in_file = open(TempFilename,"r")

        data = in_file.read(BlockSize)
        self.db.execute('INSERT INTO DataBlocks (HashKey, Data) VALUES (?, ?)',(hashlib.sha512(data).hexdigest(),sqlite3.Binary(data)))
        FileAccelerator.update(data)

        while len(data) == BlockSize:
            data = in_file.read(BlockSize)
            self.db.execute('INSERT INTO DataBlocks (HashKey, Data) VALUES (?, ?)',(hashlib.sha512(data).hexdigest(),sqlite3.Binary(data)))
            FileAccelerator.update(data)

        in_file.close()

    # debug
        #rows = self.db.execute('SELECT * FROM DataBlocks')
        #for id, HashKey, Data in rows:
        #    print type(id), id
        #    print type(HashKey), HashKey
        #    print type(Data), Data


    def __del__(self):
        " "
        self.db.commit()
        self.db.close()


class DedupFile ():
    __BlockSize__ = 16
    __PathSeparator__="\\"

    def __init__(self, FullFilename):
        " "
        # create logger
        logger = logging.getLogger("DedupDB")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        logger.addHandler(ch)
        logger.debug("# Collect file stats")
        self.__FullFilename__ = os.path.normpath(FullFilename)
        (self.__FullPath__, self.__Filename__) = os.path.split(self.__FullFilename__)
        (self.__FilenameRoot__, self.__FilenameExtension__) = os.path.splitext(self.__Filename__)
        stats = os.stat(self.__FullFilename__)
        self.__Size__ = stats[stat.ST_SIZE] #Size of the file in bytes. This is limited to 64 bits, so for large files should use the win32file.GetFileSize() function, which returns large file sizes as a long integer.
        self.__LastAccessed__ = stats[stat.ST_ATIME] # The time the file was last accessed or zero if the filesystem doesn't support this information.
        self.__LastModified__ = stats[stat.ST_MTIME] # The time the file was last modified or zero if the filesystem doesn't support this information.
        self.__Mode__ = stats[stat.ST_MODE] # Bit mask for file-mode information. The stat.S_IFDIR bit is set if path specifies a directory; the stat.S_IFREG bit is set if path specifies an ordinary file or a device.
        self.__ST_INO__ = stats[stat.ST_INO] # Not used on Windows filesystem.
        self.__ST_DEV__ = stats[stat.ST_DEV] # Drive number of the disk containing the file.
        self.__ST_NLINK__ = stats[stat.ST_NLINK] # The Visual C++ documentation is nt very helpful on this one. It simply state ''Always 1 on non-NTFS filesystem.''
        self.__ST_UID__ = stats[stat.ST_UID] # Not used on Windows.
        self.__ST_GID__ = stats[stat.ST_GID] # Not used on Windows.
        self.__ST_CTIME__ = time.ctime(stats[stat.ST_CTIME]) # The time the file was created or zero if the filesystem doesn't support this information.

        logger.debug("# Compress file to temporary")
        TempFile = tempfile.TemporaryFile()
        self.__TempFilename__ = TempFile.name
        TempFile.close()
        archive = zipfile.ZipFile(self.__TempFilename__, "w", zipfile.ZIP_DEFLATED) # "a" to append, "r" to read
        archive.write(self.__FullFilename__)
        archive.close()

    def details(self):
        print self.__FullFilename__
        print self.__FullPath__
        print self.__PathElements__
        print self.__Filename__
        print self.__FilenameRoot__
        print self.__FilenameExtension__
        print self.__Size__
        print self.__BlocksElements__
        print self.__Accelerator__.hexdigest()

    def getFullPath(self):
        return self.__FullPath__

    def getPathSeparator(self):
        return self.__PathSeparator__

    def getTempFilename(self):
        return self.__TempFilename__

    def getBlockSize(self):
        return self.__BlockSize__

    def __del__(self):
        " "

if __name__ == "__main__":
    db = DedupDB()
    mydir = 'c:\\grafik'
    #currentdir = os.curdir
    print "Browse", mydir

    if  os.path.exists(mydir):
        if os.path.isdir(mydir):
            enum = os.listdir(mydir)
            temp = os.path.join(mydir, enum[0])
            __dedupfile__ = DedupFile(temp)
            #print __dedupfile__.details()
            db.archive(__dedupfile__)
print "Done!"
exit(0)

