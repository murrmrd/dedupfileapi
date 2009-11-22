#! /usr/bin/python

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="ubuntu"
__date__ ="$22-Nov-2009 21:09:00$"



import zlib
import binascii
import sqlite3
import sys
import time
import hashlib
import logging
import os
import os.path
import stat
import tempfile
import zipfile
import json
import collections
import filecmp
import random




import ExtensionTable
import BlockTable
import ExtensionTable
import FileTreeTable
import NodeTable
import PathTable
import SequenceTable
import DedupFile


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
        self.Block = BlockTable.Block(self.db)
        self.FileTree = FileTreeTable.FileTree(self.db)
        self.PathData = PathTable.PathData(self.db)
        self.Node = NodeTable.Node(self.db)
        self.Sequence = SequenceTable.Sequence(self.db)
        self.Extension = ExtensionTable.Extension(self.db)

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
        __dedupfile__ = DedupFile.DedupFile(fullpath)

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
            
            
            self.unzip_file_into_dir(out_file.name,os.path.join(Root,os.sep))

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
        __PathSeparator__ = os.sep

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
        __PathSeparator__ = os.sep
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



if __name__ == "__main__":
    print "Hello World";
