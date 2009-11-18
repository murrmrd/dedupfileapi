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

class Extension():
    def __init__(self,db):
        " "
        self.db = db

    def create(self):
        ""
        self.db.execute('CREATE TABLE IF NOT EXISTS Extension (id INTEGER NOT NULL, Name TEXT, PRIMARY KEY (id))')

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




class FileTree():
    def __init__(self,db):
        " "
        self.db = db

    def create(self):
        ""
        self.db.execute('CREATE TABLE IF NOT EXISTS FileTree (id INTEGER NOT NULL, Parent INTEGER,PathData_id INTEGER, Node_id INTEGER NOT NULL, PRIMARY KEY (id))')

    def drop(self):
        self.db.execute('DROP TABLE IF EXISTS FileTree')

    def exist(self, PathDataID):
        ""
        str = 'SELECT count(id) FROM FileTree where PathData_id=?'
        rows = self.db.execute(str, (PathDataID, ))
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



class Block():
    def __init__(self,db):
        " "
        self.db = db

    def create(self):
        ""
        #self.db.execute('CREATE TABLE IF NOT EXISTS DataBlocks (id INTEGER NOT NULL,HashKey TEXT, Data BLOB,PRIMARY KEY (id))')
        self.db.execute('CREATE TABLE IF NOT EXISTS DataBlocks (id INTEGER NOT NULL, Data BLOB,PRIMARY KEY (id))')

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

class DedupDB ():
    ""
    __PathSeparator__ = "\\"
    logger = logging.getLogger("DedupDB")
    db = sqlite3.connect(':memory:')


    def init_logger(self):
        ""
        # create logger
        self.logger.setLevel(logging.INFO)
        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        self.logger.addHandler(ch)

    def open_database(self):
        ""
        #self.logger.debug("Connect to db")
        self.db = sqlite3.connect('database')

    def create_tables(self):
        ""
        #self.logger.debug("Create tables")
        self.FileTree.create()
        self.PathData.create()
        self.Sequence.create()
        self.Node.create()
        self.Block.create()
        self.Extension.create()

    def drop_tables(self):
        ""
        #self.logger.debug("Drop tables")
        self.FileTree.drop()
        self.PathData.drop()
        self.Node.drop()
        self.Block.drop()
        self.Sequence.drop()
        self.Extension.drop()

    def __init__(self):
        " "
        self.init_logger()
        self.open_database()
        self.Block = Block(self.db)
        self.FileTree = FileTree(self.db)
        self.PathData = PathData(self.db)
        self.Node = Node(self.db)
        self.Sequence = Sequence(self.db)
        self.Extension = Extension(self.db)

        self.drop_tables()
        self.create_tables()

    def fill_pathdata_table(self,PathElement):
        #self.logger.debug("Fill PathData table")
        if not(self.PathData.exist(PathElement)):
            #self.logger.debug("Element %s does not exist",PathElement)
            self.PathData.insert(PathElement)

    def fill_node_table(self):
        print "fill_node_table : not implemented"
        exit()

    def fill_filetree_table(self,fullpath,filenameroot, pathseparator):
        ""
        #self.logger.debug("Fill FileTree table")
        fullfilename = fullpath+pathseparator+filenameroot
        PathElements = fullfilename.split(pathseparator)
        parent=-1
        node=-1
        parent = -1
        for PathElement in PathElements:
            PathDataID=self.PathData.query(PathElement)
            if not(self.FileTree.exist(PathDataID)):
                self.FileTree.insert(parent,PathDataID,node)
            parent= self.FileTree.query(parent,PathDataID)
        return parent

    def archive(self, fullpath):
        #self.logger.debug("# Prepare path for Deduplication")
        # process path to build a dictionnary for path
        # exemple: c:\grafik => {'hash("c:"):"c:"','hash("grafik"):"grafik"'}
        __dedupfile__ = DedupFile(fullpath)

        __FullFilename__ = __dedupfile__.getfullfilename()
        (__FullPath__, __FileName__) = os.path.split(__FullFilename__)
        (__FilenameRoot__, __Extension__) = os.path.splitext(__FileName__)
        __PathSeparator__ = __dedupfile__.getpathseparator()
        __Size__ =  __dedupfile__.getsize()
        __CompressedSize__ = __dedupfile__.getcompressedsize()
        __FullCompressedFilename__ = __dedupfile__.gettempfilename()
        __CreateTime__ =  __dedupfile__.getcreatetime()
        __LastAccessedTime__ =  __dedupfile__.getlastaccessedtime()
        __LastModifiedTime__ =  __dedupfile__.getlastmodifiedtime()
        __CompressedFlag__ = True

        # Fill PathElement table
        print "*** Process path elements"
        PathElements = __FullPath__.split(__PathSeparator__)
        for PathElement in PathElements:
            self.fill_pathdata_table(PathElement)
        self.fill_pathdata_table(__FilenameRoot__)

        # Fill Extension table
        print "*** Process file extension"
        if not(self.Extension.exist(__Extension__)):
            self.Extension.insert(__Extension__)
        __ExtensionID__ = self.Extension.query(__Extension__)

        # Fill FileTree table
        print "*** Process file tree"
        __FileTreeID__ = self.fill_filetree_table(__FullPath__,__FilenameRoot__,__PathSeparator__)

        print "*** Build file sequence"
        SequenceDict=[]
        #self.logger.debug("Split temporary compressed file into blocks")
        if (__CompressedSize__>__Size__):
            TempFilename = __FullFilename__
            __CompressedFlag__ = False
        else:
            TempFilename = __FullCompressedFilename__

#       FileAccelerator = hashlib.sha512()
        BlockSize = __dedupfile__.getBlockSize()
        in_file = open(TempFilename, "rb")
        data = in_file.read(BlockSize)

        #print ''.join("%02x " % ord(c) for c in data)

        #if not(self.Block.exist(hashlib.sha512(data).hexdigest())):
        if not(self.Block.exist_block(data)):
            self.Block.insert(data)
#        SequenceDict.append(self.Block.query(hashlib.sha512(data).hexdigest()))
        SequenceDict.append(self.Block.query_block(data))
        #       FileAccelerator.update(data)
        while len(data) == BlockSize:
            data = in_file.read(BlockSize)
            #print ''.join("%02x " % ord(c) for c in data)
#            if not(self.Block.exist(hashlib.sha512(data).hexdigest())):
            if not(self.Block.exist_block(data)):
                #print "**** Store new block"
                self.Block.insert(data)
            else:
                print "**** Existing block reused"
#            SequenceDict.append(self.Block.query(hashlib.sha512(data).hexdigest()))
            SequenceDict.append(self.Block.query_block(data))
#           FileAccelerator.update(data)
        in_file.close()

        if not(self.Sequence.exist(SequenceDict)):
            self.Sequence.insert(SequenceDict)
        __SequenceID__=self.Sequence.query(SequenceDict)

        #Create Node item and return NodeID
        print "*** Create file node"
        self.Node.insert(__SequenceID__,__ExtensionID__,__Size__,__CreateTime__,__LastAccessedTime__,__LastModifiedTime__,__CompressedFlag__)
        __NodeID__ = self.Node.query(__SequenceID__)

        #update FileTree row where FileTreeid=FileTreeID with FileTree.NodeID=NodeID
        self.FileTree.update(__FileTreeID__,__NodeID__)

        # flush changes to db
        print "*** Commit changes to database"
        self.db.commit()

    def unzip_file_into_dir(self,file, dir):
        #os.mkdir(dir, 0777)
        
        zfobj = zipfile.ZipFile(file)
        
        for name in zfobj.namelist():
            if name.endswith('/'):
                #os.mkdir(os.path.join(dir, name))
                ""
            else:
                outfile = open(os.path.join(dir, name), 'wb')
                outfile.write(zfobj.read(name))
                outfile.close()

    def restore(self, NodeID):
        "restore fil referenced by NodeID to original location"
        FullPath = self.getfullpath(NodeID)
        (id,BlockSequence_id,Extension_id,size,createtime,lastmodifiedtime,lastaccessedtime, compressedflag) = self.Node.get(NodeID)
        SequenceDict = self.Sequence.get_sequence(BlockSequence_id)
        

#        print NodeID
#        print FullPath
#        print id,BlockSequence_id,Extension_id,size,createtime,lastmodifiedtime,lastaccessedtime, compressedflag
#        print id, SequenceData
#        print SequenceDict
        
        
        #TempFile = tempfile.TemporaryFile()
        #self.__TempFilename__ = TempFile.name
        
        if (compressedflag):
            " file was compressed so we rebuild a temporary compressed file"
            temporaryfile = tempfile.TemporaryFile()
            temporaryfilename = temporaryfile.name
            temporaryfile.close()

            out_file = open(temporaryfilename,"wb")
            #out_file = open("c:\\grafik\\test.zip","wb")
        else:
            out_file = open(FullPath,"wb")

        
        for BlockID in SequenceDict:
            #(id,HashKey,data) =  self.Block.get(BlockID)
            (id,data) =  self.Block.get(BlockID)
            #print ''.join("%02x " % ord(c) for c in data)
            
            out_file.write(data)
        out_file.flush()
        out_file.close()

        if (compressedflag):
            " file was compressed so we rebuild a temporary compressed file"
            (Path,FileName) = os.path.split(FullPath)
            #print out_file.name,Path
            #print FullPath, Path, FileName,
            (Root,Tail) = os.path.splitdrive(FullPath)
            self.unzip_file_into_dir(out_file.name,os.path.join(Root,self.__PathSeparator__))
        
    def getfullpath(self,NodeID):
       
        (id,BlockSequence_id,Extension_id,size,createtime,lastmodifiedtime,lastaccessedtime, compressedflag) = self.Node.get(NodeID)
        st = ""
        (id,Extension) = self.Extension.get(Extension_id)
        st = st +  Extension
        (FileTreeID,Parent,PathData_id,Node_id) = self.FileTree.getbynodeid(NodeID)
        (id,Name) = self.PathData.get(PathData_id)
        st = Name + st

        while (Parent != -1):
            (FileTreeID,Parent,PathData_id,Node_id) = self.FileTree.get(Parent)
            (id,Name) = self.PathData.get(PathData_id)
            st = Name +self.__PathSeparator__+ st
        return st

    def printnodedetails(self,NodeID):
        "print out information about file node"
        st = self.getfullpath(NodeID)
        (id,BlockSequence_id,Extension_id,size,createtime,lastmodifiedtime,lastaccessedtime, compressedflag) = self.Node.get(NodeID)
        st = "F"+"|"+st +"|"+ str(size)+"|"+time.ctime(createtime)+"|"+time.ctime(lastmodifiedtime)+"|"+time.ctime(lastaccessedtime)
        print st

    def printfiletreedetails(self,NodeID):
        "print out information about filetree i.e. directory"
        __PathSeparator__ = "\\"

        st = ""
        (FileTreeID,Parent,PathData_id,Node_id) = self.FileTree.get(NodeID)
        (id,Name) = self.PathData.get(PathData_id)
        st = Name + st

        while (Parent != -1):
            (FileTreeID,Parent,PathData_id,Node_id) = self.FileTree.get(Parent)
            (id,Name) = self.PathData.get(PathData_id)
            st = Name +__PathSeparator__+ st

        st = "D"+"|"+st
        print st

    def listfiletree(self,TreeID,toscreen):
        NodeIDlist=[]
        " Print all elements in directory described by FileTree row 'TreeID'"
        (FileTreeID,Parent,PathData_id,Node_id) = self.FileTree.get(TreeID)
        if (Node_id==-1):
           " is directory"
           if toscreen:
            self.printfiletreedetails(FileTreeID)
        else:
           " is file"
           if toscreen:
            self.printnodedetails(Node_id)
           NodeIDlist.append(Node_id)


        rows = self.FileTree.getlistbyparent(TreeID)
        for (FileTreeID,Parent,PathData_id,Node_id) in rows:
            #(FileTreeID,Parent,PathData_id,Node_id) = rows.fetch()
            if (Node_id==-1):
                " is directory"
                if toscreen:
                    self.printfiletreedetails(FileTreeID)
            else:
                " is file"
                if toscreen:
                    self.printnodedetails(Node_id)
                NodeIDlist.append(Node_id)
        return NodeIDlist


    def getfiletreeidbypath(self,DirPath):
        " Return FileTree row id for path 'DirPath'"
        __PathSeparator__ = "\\"
        ParentID = -1
        PathElements = DirPath.split(__PathSeparator__)
        for PathElement in PathElements:
            (PathID, Name) = self.PathData.getbyname(PathElement)
            FileTreeID = self.FileTree.query(ParentID,PathID)
            (FileTreeID,ParentID,PathID,Node_id) = self.FileTree.get(FileTreeID)
            ParentID = FileTreeID
        return FileTreeID





    def __del__(self):
        ""
        self.db.commit()
        self.db.close()

class DedupFile ():
    ""
    __BlockSize__ = 256
    __PathSeparator__ = "\\"

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
        #logger.debug("# Collect file stats")
        self.__FullFilename__ = os.path.normpath(FullFilename)

        print "*** Get files stats"
        stats = os.stat(self.__FullFilename__)
        self.__Size__ = stats[stat.ST_SIZE] #Size of the file in bytes. This is limited to 64 bits, so for large files should use the win32file.GetFileSize() function, which returns large file sizes as a long integer.
        self.__LastAccessed__ = stats[stat.ST_ATIME] # The time the file was last accessed or zero if the filesystem doesn't support this information.
        self.__LastModified__ = stats[stat.ST_MTIME] # The time the file was last modified or zero if the filesystem doesn't support this information.
        #self.__Mode__ = stats[stat.ST_MODE] # Bit mask for file-mode information. The stat.S_IFDIR bit is set if path specifies a directory; the stat.S_IFREG bit is set if path specifies an ordinary file or a device.
        #self.__ST_INO__ = stats[stat.ST_INO] # Not used on Windows filesystem.
        #self.__ST_DEV__ = stats[stat.ST_DEV] # Drive number of the disk containing the file.
        #self.__ST_NLINK__ = stats[stat.ST_NLINK] # The Visual C++ documentation is nt very helpful on this one. It simply state ''Always 1 on non-NTFS filesystem.''
        #self.__ST_UID__ = stats[stat.ST_UID] # Not used on Windows.
        #self.__ST_GID__ = stats[stat.ST_GID] # Not used on Windows.
        self.__ST_CTIME__ = stats[stat.ST_CTIME] # The time the file was created or zero if the filesystem doesn't support this information.

        #logger.debug("# Compress file to temporary")
        print "*** Compress file to temporary"
        TempFile = tempfile.TemporaryFile()
        self.__TempFilename__ = TempFile.name
        TempFile.close()
        archive = zipfile.ZipFile(self.__TempFilename__, "w", zipfile.ZIP_DEFLATED) # "a" to append, "r" to read
        archive.write(self.__FullFilename__)
        archive.close()

        print "*** Get Compressed file stats"
        CompressedFileStats = os.stat(self.__TempFilename__)
        self.__CompressedSize__ = CompressedFileStats[stat.ST_SIZE] #Size of the file in bytes. This is limited to 64 bits, so for large files should use the win32file.GetFileSize() function, which returns large file sizes as a long integer.

    def details(self):
        ""
        print self.getfullfilename()
        print self.getpathseparator()
        print self.gettempfilename()
        print self.getsize()
        print time.ctime(self.getcreatetime())
        print time.ctime(self.getlastaccessedtime())
        print time.ctime(self.getlastmodifiedtime())
        print self.getBlockSize()
        #print self.__FullPath__
        #print self.__PathElements__
        #print self.__Filename__
        #print self.__FilenameRoot__
        #print self.__FilenameExtension__
        #print self.__Size__
        #print self.__BlocksElements__
        #print self.__Accelerator__.hexdigest()

#    def getFileName(self):
#        ""
#        return self.__Filename__

#    def getFullPath(self):
#        ""
#        return self.__FullPath__

    def getfullfilename(self):
        ""
        return self.__FullFilename__

    def getsize(self):
        ""
        return self.__Size__

    def getcompressedsize(self):
        ""
        return self.__CompressedSize__

    def getpathseparator(self):
        ""
        return self.__PathSeparator__

    def gettempfilename(self):
        ""
        return self.__TempFilename__

    def getlastaccessedtime(self):
        ""
        return self.__LastAccessed__

    def getlastmodifiedtime(self):
        ""
        return self.__LastModified__

    def getcreatetime(self):
        ""
        return self.__ST_CTIME__

    def getBlockSize(self):
        ""
        return self.__BlockSize__

    def __del__(self):
        ""

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


if __name__ == "__main__":

    startDir = 'c:\\grafik'

    print "Remove ",startDir
    removeall(startDir)
    
    mkfile('c:\\grafik\\test.txt',500*1000)

    print "Prepare DB"
    db = DedupDB()


    print "** Dedup file: "
    db.archive('c:\\grafik\\test.txt')
    
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
    (FullPath,FileName) = os.path.split('c:\\grafik\\test.txt')
    (FilenameRoot,Extension) = os.path.splitext(FileName)
    newfile = 'c:\\grafik\\test.txt'.replace(Extension, '~'+Extension)

    if os.path.exists(newfile):
        os.remove(newfile)
    os.rename('c:\\grafik\\test.txt',newfile)

    TreeID = db.getfiletreeidbypath('c:\\grafik')
    NodeIDList = db.listfiletree(TreeID,False)
    for NodeID in NodeIDList:
            restoredfilename = db.getfullpath(NodeID)
            (FullPath,FileName) = os.path.split(restoredfilename)
            (FilenameRoot,Extension) = os.path.splitext(FileName)
            backupfilename =  restoredfilename.replace(Extension, '~'+Extension)
            print "** Restore Node ",NodeID, " = ", restoredfilename
            db.restore(NodeID)
            comparefiles(backupfilename,restoredfilename)
    
    stats = os.stat("c:\\grafik\\test.txt")
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
    
    exit()
    

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
    exit()
    

