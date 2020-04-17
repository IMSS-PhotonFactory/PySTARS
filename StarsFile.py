# Functions for file handling
from pathlib import Path

def getfilepath(filedir, filename):
    return Path(filedir, filename)

def checkfileexist(filename, serverdir, libdir):
    libpath = Path(serverdir, libdir)
    filepath = Path(libpath, filename)
    return bool(filepath.is_file())

def loadfiletolist(filename, serverdir, libdir):
    filecontent = []
    libpath = Path(serverdir, libdir)
    filepath = Path(libpath, filename)
    with open(filepath, "r") as fobj:
        for line in fobj:
            if (not line.startswith("#")) or (not line.split()):
                filecontent.append(line.strip())
    return filecontent

def loadfiletodictionary(filename, serverdir, libdir, dict_aliasreal, dict_realalias):
    aliasreal = []
    libpath = Path(serverdir, libdir)
    filepath = Path(libpath, filename)
    with open(filepath, "r") as fobj:
        for line in fobj:
            if (not line.startswith("#")) or (not line.split()):
                aliasreal = line.split()
                dict_aliasreal[aliasreal[0]] = aliasreal[1]
                dict_realalias[aliasreal[1]] = aliasreal[0]

def loadkeyfile(filename, serverdir, libdir):
    filecontent = []
    libpath = Path(serverdir, libdir)
    filepath = Path(libpath, filename)
    with open(filepath, "r") as fobj:
        for line in fobj:
            filecontent.append(line.strip())
    return filecontent
