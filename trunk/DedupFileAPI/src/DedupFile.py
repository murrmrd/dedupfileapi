#! /usr/bin/python

# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="ubuntu"
__date__ ="$22-Nov-2009 21:07:55$"

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



if __name__ == "__main__":
    print "Hello World";
