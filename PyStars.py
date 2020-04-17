#!/usr/bin/python3
# STARS Server in Python
# Based on Perl STARS server from Takashi Kosuge; KEK Tsukuba
# stars.kek.jp

__author__ = 'Jan Szczesny'
__version__ = '1.33'
__date__ = '2020.04.17'

import sys
import os
import select
import socket
import time
import re
import random
import logging
from logging.handlers import RotatingFileHandler
import configparser
from argparse import ArgumentParser
import StarsFile

RNDMAX = 10000
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 6057
TCP_BUFFER_SIZE = 512

LIBDIR = 'takaserv-lib'
KEYDIR = None
DEFAULT_LOGFILE = 'server.log'
DEFAULT_LOGLEVEL = logging.INFO
DEFAULT_LOGSIZE = 2 #MB
DEFAULT_LOGBACKUP = 2 #Files
CONFIGFILE = 'PyStars.cfg'

HOSTLIST = 'allow.cfg'
ALIASES = 'aliases.cfg'
CMDDENY = 'command_deny.cfg'
CMDALLOW = 'command_allow.cfg'
RECONNECTABLEDENY = 'reconnectable_deny.cfg'
RECONNECTABLEALLOW = 'reconnectable_allow.cfg'

class Stars:
    def __init__(self, port=DEFAULT_PORT, lib=LIBDIR, key=KEYDIR, logfile=DEFAULT_LOGFILE, loglevel=DEFAULT_LOGLEVEL, logsize=DEFAULT_LOGSIZE, logbackup=DEFAULT_LOGBACKUP):
        self._version = __version__
        self._host = DEFAULT_HOST
        self._port = port
        self._libdir = lib
        if (key is None) or (key == ''):
            self._keydir = lib
        else:
            self._keydir = key
        self._logfile = StarsFile.getfilepath(os.path.dirname(os.path.realpath(__file__)), logfile)
        self._level = loglevel

        self._node = {}
        self._node_h = {}
        self._node_buf = {}
        self._node_flgon = {}
        self._node_idkey = {}
        self._aliasreal = {}
        self._realalias = {}

        self._readable = []
        self._writeable = []
        self._writebuf = {}
        self._cmddeny = []
        self._cmdallow = []
        self._reconndeny = []
        self._reconnallow = []
        self._initialized = True

        self._logger = logging.getLogger("ServerLog")
        self._logger.setLevel(self._level)
        loghandler = RotatingFileHandler(self._logfile, maxBytes=1000*1024*logsize, backupCount=logbackup)
        logformat = "{asctime} [{levelname:8}] -> {message}"
        formatter = logging.Formatter(logformat, style="{")
        loghandler.setFormatter(formatter)
        self._logger.addHandler(loghandler)

    def runserver(self):
        try:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setblocking(0)
            listener.bind((self._host, self._port))
            listener.listen()
        except Exception as ex:
            self._logger.exception('Can\'t create socket for listining! %s', ex)
            return False
        self._readable.append(listener)
        while True:
            ready, write, exept_unused = select.select(self._readable, self._writeable, [], 2)
            for s in ready:
                if s is listener:
                    new_sock, unused = s.accept()
                    new_sock.setblocking(0)
                    self._readable.append(new_sock)
                    bufhn, ipadr = self._system_gethostname_or_ip(new_sock)
                    if not self._system_checkhost(HOSTLIST, bufhn, ipadr, False):
                        self._add_to_send(new_sock, "Bad host. %s\n" %bufhn)
                        self._readable.remove(new_sock)
                        new_sock.close()
                    self._node_buf[new_sock] = ''
                    self._node_idkey[new_sock] = self._get_nodeidkey()
                    self._add_to_send(new_sock, "%s\n" %self._node_idkey[new_sock])
                else:
                    #To handle large data
                    datafragments = []
                    try:
                        while True:
                            datapiece = s.recv(TCP_BUFFER_SIZE).decode("utf8")
                            self._logger.debug('recv: %s', datapiece.strip())
                            if not datapiece:
                                break
                            datafragments.append(datapiece)
                    except:
                        #Ignore the exception!
                        pass
                    data = ''.join(datafragments)
                    if (len(data) != 0):
                        self._node_buf[s] = data
                        m = re.findall(r"([^\r\n]*)\r*\n", self._node_buf[s])
                        for buf in m:
                            if re.match(r"(?i)^(exit|quit)", buf):
                                self._delnode(s)
                                self._readable.remove(s)
                                if s in self._writeable:
                                    self._writeable.remove(s)
                                    del self._writebuf[s]
                                s.close()
                            elif s in self._node_h:
                                self._sendmes(s, buf)
                            else:
                                if not self._addnode(s, buf):
                                    self._logger.debug('close socket: %s', s)
                                    del self._node_buf[s]
                                    del self._node_idkey[s]
                                    self._readable.remove(s)
                                    if s in self._writeable:
                                        self._writeable.remove(s)
                                        del self._writebuf[s]
                                    s.close()
                    else:
                        self._logger.error('Connection lost. %s', s)
                        self._delnode(s)
                        self._readable.remove(s)
                        if s in self._writeable:
                            self._writeable.remove(s)
                            del self._writebuf[s]
                        s.close()
            for s in write:
                if self._printh(s):
                    self._writeable.remove(s)
                    del self._writebuf[s]

    def _check_term_and_host(self, nd, hd):
        self._logger.debug('check_term_and_host I am node %s', nd)
        if not StarsFile.checkfileexist(nd + '.allow', os.path.dirname(os.path.realpath(__file__)), self._libdir):
            self._logger.debug('check_term_and_host not_found_ok %s.allow', nd)
            return True
        host, ip = self._system_gethostname_or_ip(hd)
        if self._system_checkhost('.allow', host, ip, True):
            return True
        return False

    def _check_nodekey(self, nname, nkeynum, nkeyval):
        kcount = 0
        if not StarsFile.checkfileexist(nname + '.key', os.path.dirname(os.path.realpath(__file__)), self._keydir):
            return False
        kfile = StarsFile.loadkeyfile(nname + '.key', os.path.dirname(os.path.realpath(__file__)), self._keydir)
        kcount = kfile.__len__()
        if kcount == 0:
            return False
        kcount = nkeynum % kcount
        if kfile[kcount] == nkeyval:
            return True
        return False

    @classmethod
    def _get_nodeidkey(cls):
        return random.randint(0, RNDMAX)

    def _add_to_send(self, xfh, xbuf):
        if xfh not in self._writeable:
            self._writeable.append(xfh)
            self._writebuf[xfh] = ''
        self._writebuf[xfh] = xbuf
        if 'Debugger' in self._node:
            handle = self._node['Debugger']
            if handle not in self._writeable:
                self._writeable.append(handle)
                self._writebuf[handle] = ''
            self._writebuf[handle] += xbuf

    def _printh(self, xfh):
        self._logger.debug('Transmit %s to %s', self._writebuf[xfh].strip(), (self._system_gethostname_or_ip(xfh, 'ip')))
        buf = (self._writebuf[xfh]).encode()
        if len(buf) == 0:
            return True
        send = xfh.send(buf)
        self._writebuf[xfh] = (buf[send:]).decode()
        return False

    def _sendmes(self, handle, buf):
        fromnode = fromnodes = self._node_h[handle]
        m = re.match(r"^([a-zA-Z_0-9.\-]+)>", buf)
        buf = re.sub(r"^([a-zA-Z_0-9.\-]+)>", '', buf, count=1)
        if m:
            fromnode = m.group(1)
        m = re.match(r"^([a-zA-Z_0-9.\-]+)\s*", buf)
        buf = re.sub(r"^([a-zA-Z_0-9.\-]+)\s*", '', buf, count=1)
        if not m:
            self._add_to_send(handle, "System>%s> @\n" %fromnode)
            return True
        tonodes = m.group(1)
        if tonodes in self._aliasreal:
            tonodes = self._aliasreal[tonodes]
        if (re.match(r"^[^@]", buf)) and (((self._cmddeny) and (self._isdenycheckcmd_deny(fromnodes, tonodes, buf)))\
            or ((self._cmdallow) and (self._isdenycheckcmd_allow(fromnodes, tonodes, buf)))):
            if re.match(r"^[^_]", buf):
                self._add_to_send(handle, "System>%s @%s Er: Command denied.\n" %(fromnode, buf))
            return False
        tonode = tonodes
        tonode = tonode.split('.', 1)[0]
        if tonode == 'System':
            return self._system_commands(handle, fromnode, buf)
        if not tonode in self._node:
            if not re.match(r"^[_@]", buf):
                self._add_to_send(handle, "System>%s @%s Er: %s is down.\n" %(fromnode, buf, tonode))
            return False
        if fromnode in self._realalias:
            fromnode = self._realalias[fromnode]
        tonodeh = self._node[tonode]
        self._add_to_send(tonodeh, "%s>%s %s\n" %(fromnode, tonodes, buf))
        return True

    def _isdenycheckcmd_deny(self, frm, to, buf):
        buf = re.search(r"^(\S+)( |$)", buf)
        if not buf:
            return True
        buf = "%s>%s %s" %(frm, to, buf.group())
        for chk in self._cmddeny:
            if(re.findall(chk, buf)):
                return True
        return False

    def _isdenycheckcmd_allow(self, frm, to, buf):
        buf = re.search(r"^(\S+)( |$)", buf)
        if not buf:
            return True
        buf = "%s>%s %s" %(frm, to, buf.group())
        for chk in self._cmdallow:
            if (re.findall(chk, buf)):
                return False
        return True

    def _isdennycheckreconnecttable_deny(self, node, host):
        for chk in self._reconndeny:
            if (re.match(r"^%s\s+%s$" %(node, host), chk)) or (re.match(r"^%s$" %node, chk)):
                return True
        return False

    def _isdennycheckreconnecttable_allow(self, node, host):
        for chk in self._reconnallow:
            if (re.match(r"^%s\s+%s$" %(node, host), chk)) or (re.match(r"^%s$" %node, chk)):
                return False
        return True

    def _system_commands(self, hd, frn, cmd):
        if cmd.startswith('_'):
            self._system_event(frn, cmd)
        elif re.match(r"disconnect ", cmd):
            cmd = cmd.replace('disconnect ', '')
            self._system_disconnect(hd, frn, cmd)
        elif re.match(r"flgon ", cmd):
            cmd = cmd.replace('flgon ', '')
            self._system_flgon(hd, frn, cmd)
        elif re.match(r"flgoff ", cmd):
            cmd = cmd.replace('flgoff ', '')
            self._system_flgoff(hd, frn, cmd)
        elif cmd == 'loadreconnectablepermission':
            self._system_loadreconnecttablepermission()
            self._add_to_send(hd, "System>%s @loadreconnectablepermission Reconnectable permission list has been loaded.\n" %frn)
        elif cmd == 'loadpermission':
            self._system_loadcommandpermission()
            self._add_to_send(hd, "System>%s @loadpermission Command permission list has been loaded.\n" %frn)
        elif cmd == 'loadaliases':
            self._system_loadaliases()
            self._add_to_send(hd, "System>%s @loadaliases Aliases has been loaded.\n" %frn)
        elif cmd == 'listaliases':
            self._add_to_send(hd, "System>%s @listaliases %s\n" %(frn, self._system_listaliases()))
        elif cmd == 'listnodes':
            self._add_to_send(hd, "System>%s @listnodes %s\n" %(frn, self._system_listnodes()))
        elif cmd == 'gettime':
            self._add_to_send(hd, "System>%s @gettime %s\n" %(frn, self._system_gettime()))
        elif cmd == 'hello':
            self._add_to_send(hd, "System>%s @hello Nice to meet you.\n" %frn)
        elif cmd == 'getversion':
            self._add_to_send(hd, "System>%s @getversion %s\n" %(frn, self._version))
        elif cmd == 'help':
            self._add_to_send(hd, "System>%s @help flgon flgoff loadaliases listaliases loadpermission loadreconnectablepermission listnodes gettime hello getversion disconnect\n" %frn)
        elif cmd.startswith('@'):
            return True
        else:
            self._add_to_send(hd, "System>%s @%s Er: Command is not found or parameter is not enough.\n" %(frn, cmd))
        return True

    def _system_event(self, frn, cmd):
        to = None
        topre = None
        if frn in self._realalias:
            frn = self._realalias[frn]
        for key in self._node_flgon:
            if re.match(frn, self._node_flgon[key]):
                to = topre = key
                topre = topre.split('.', 1)[0]
                buffh = self._node[topre]
                self._add_to_send(buffh, "%s>%s %s\n" %(frn, to, cmd))

    def _system_disconnect(self, hd, frn, cmd):
        if not re.match(r"^([a-zA-Z_0-9.\-]+)", cmd):
            self._add_to_send(hd, "System>%s @disconnect Er: Parameter is not enough.\n" %frn)
            return False
        if cmd in self._aliasreal:
            cmd = self._aliasreal[cmd]
        if not cmd in self._node:
            self._add_to_send(hd, "System>%s @disconnect Er: Node %s is down.\n" %(frn, cmd))
            return False
        dhandle = self._node[cmd]
        self._add_to_send(hd, "System>%s @disconnect %s.\n" %(frn, cmd))
        self._delnode(dhandle)
        self._readable.remove(dhandle)
        if dhandle in self._writeable:
            self._writeable.remove(dhandle)
        dhandle.close()
        return True

    def _system_flgon(self, hd, frn, cmd):
        if not re.match(r"^([a-zA-Z_0-9.\-]+)", cmd):
            self._add_to_send(hd, "System>%s @flgon Er: Parameter is not enough.\n" %frn)
            return False
        if frn in self._node_flgon:
            if (re.findall(cmd, self._node_flgon[frn])):
                self._add_to_send(hd, "System>%s @flgon Er: Node %s is allready in the list.\n" %(frn, cmd))
                return False
            self._node_flgon[frn] += ' ' + cmd
            self._add_to_send(hd, "System>%s @flgon Node %s has been registered.\n" %(frn, cmd))
            return True
        self._node_flgon[frn] = cmd
        self._add_to_send(hd, "System>%s @flgon Node %s has been registered.\n" %(frn, cmd))
        return True

    def _system_flgoff(self, hd, frn, cmd):
        if not re.match(r"^([a-zA-Z_0-9.\-]+)", cmd):
            self._add_to_send(hd, "System>%s @flgoff Er: Parameter is not enough.\n" %frn)
            return False
        if frn in self._node_flgon:
            if (re.findall(cmd, self._node_flgon[frn])):
                self._node_flgon[frn] = self._node_flgon[frn].replace(cmd, '').lstrip()
                self._add_to_send(hd, "System>%s @flgoff Node %s has been removed.\n" %(frn, cmd))
                return True
            else:
                self._add_to_send(hd, "System>%s @flgoff Er: Node %s is not in the list.\n" %(frn, cmd))
                return False
        else:
            self._add_to_send(hd, "System>%s @flgoff Er: List is void.\n" %frn)
            return False

    def _system_gethostname_or_ip(self, sock, value='both'):
        try:
            ip, unused = sock.getpeername()
            hostname = socket.gethostbyaddr(ip)
            if value == 'ip':
                return ip
            if value == 'host':
                return hostname[0]
            if value == 'both':
                return hostname[0], ip
            return None
        except Exception as ex:
            self._logger.exception('Unable to get hostname and ip. %s', ex)

    def _system_checkhost(self, l, hostname, ipadr, unchecked):
        try:
            self._logger.debug('system_checkhost: I am %s and %s', hostname, ipadr)
            check = [hostname]
            if hostname != ipadr:
                check.append(ipadr)
            allowedhost = StarsFile.loadfiletolist(l, os.path.dirname(os.path.realpath(__file__)), self._libdir)
            for entry in allowedhost:
                self._logger.debug('system_checkhost: Checking with %s', entry)
                if (re.match(entry, check[0])) or (re.match(entry, check[1])):
                    self._logger.debug('system_checkhost: Matched with %s', entry)
                    return True
            self._logger.debug('system_checkhost: No match found!')
            return unchecked
        except Exception as ex:
            self._logger.exception('Unable to checkhost. %s', ex)

    @classmethod
    def _system_gettime(cls, tin=None):
        tlocal = time.localtime(tin)
        return time.strftime('%Y-%m-%d %H:%M:%S', tlocal)

    def _system_listnodes(self):
        return " ".join(self._node)

    def _system_listaliases(self):
        return " ".join(self._aliasreal.values())

    def _check_reconnecttable(self, node, hd):
        if (not self._reconndeny) and (not self._reconnallow):
            return False
        if ((self._reconndeny) and (self._isdennycheckreconnecttable_deny(node, self._system_gethostname_or_ip(hd, 'host'))))\
            or ((self._reconnallow) and (self._isdennycheckreconnecttable_allow(node, self._system_gethostname_or_ip(hd, 'host')))):
            return False
        return True

    def _disconnect_for_reconnect(self, node):
        cmd = node
        dhandle = self._node[cmd]
        self._delnode(dhandle)
        self._readable.remove(dhandle)
        if dhandle in self._writeable:
            self._writeable.remove(dhandle)
        dhandle.close()
        return True

    def _addnode(self, handle, buff):
        try:
            node, idmess = buff.split(' ')
        except Exception:
            self._logger.exception("Node name or keyword missing.")
            return False
        reconnectflag = False
        if node in self._node:
            if not self._check_reconnecttable(node, handle):
                self._add_to_send(handle, "System> Er: %s already exists.\n" %node)
                return False
            else:
                reconnectflag = True
        if not self._check_term_and_host(node, handle):
            self._add_to_send(handle, "System> Er: Bad host for %s\n" %node)
            return False
        if not self._check_nodekey(node, self._node_idkey[handle], idmess):
            self._add_to_send(handle, "System> Er: Bad node name or key\n")
            return False
        if reconnectflag:
            self._disconnect_for_reconnect(node)
        self._node[node] = handle
        self._node_h[handle] = node
        self._add_to_send(handle, "System>%s Ok:\n" %node)
        if node in self._realalias:
            node = self._realalias[node]
        for key in self._node_flgon:
            if re.match(node, self._node_flgon[key]):
                to = topre = key
                topre = topre.split('.', 1)[0]
                buffh = self._node[topre]
                self._add_to_send(buffh, "%s>%s _Connected\n" %(node, key))
        return True

    def _delnode(self, handle):
        try:
            to = None
            topre = None
            node = self._node_h.get(handle)
            if node is None:
                return
            self._logger.debug("Node %s will be removed from list.", node)
            del self._node_h[handle]
            del self._node[node]
            del self._node_buf[handle]
            del self._node_idkey[handle]
            for key in list(self._node_flgon.keys()):
                if re.findall(r"%s($|.)" %node, key):
                    del self._node_flgon[key]
            if node in self._realalias:
                node = self._realalias[node]
            for key in self._node_flgon:
                if re.match(node, self._node_flgon[key]):
                    to = topre = key
                    topre = topre.split('.', 1)[0]
                    buffh = self._node[topre]
                    self._add_to_send(buffh, "%s>%s _Disconnected\n" %(node, key))
        except Exception as ex:
            self._logger.exception("Exception occurred: %s", ex)

    def _system_loadcommandpermission(self):
        try:
            self._logger.debug('Loading command permissions')
            self._cmddeny = StarsFile.loadfiletolist(CMDDENY, os.path.dirname(os.path.realpath(__file__)), self._libdir)
            self._logger.debug('CMDDENY loaded: %s', self._cmddeny)
            self._cmdallow = StarsFile.loadfiletolist(CMDALLOW, os.path.dirname(os.path.realpath(__file__)), self._libdir)
            self._logger.debug('CMDALLOW loaded: %s', self._cmdallow)
        except Exception as ex:
            self._logger.exception('Exception occurred: %s', ex)
            self._initialized = False

    def _system_loadaliases(self):
        try:
            self._logger.debug('Loading aliases')
            StarsFile.loadfiletodictionary(ALIASES, os.path.dirname(os.path.realpath(__file__)), self._libdir, self._aliasreal, self._realalias)
            self._logger.debug('AliasReal loaded: %s', self._aliasreal)
            self._logger.debug('RealAlias loaded: %s', self._realalias)
        except Exception as ex:
            self._logger.exception('Exception occurred: %s', ex)
            self._initialized = False

    def _system_loadreconnecttablepermission(self):
        try:
            self._logger.debug('Loading reconnect table permissions')
            self._reconndeny = StarsFile.loadfiletolist(RECONNECTABLEDENY, os.path.dirname(os.path.realpath(__file__)), self._libdir)
            self._logger.debug('RECONNECTABLEDENY loaded: %s', self._reconndeny)
            self._reconnallow = StarsFile.loadfiletolist(RECONNECTABLEALLOW, os.path.dirname(os.path.realpath(__file__)), self._libdir)
            self._logger.debug('RECONNECTABLEALLOW loaded: %s', self._reconnallow)
        except Exception as ex:
            self._logger.exception('Exception occurred: %s', ex)
            self._initialized = False

    def startup(self):
        self._initialized = True
        self._logger.info('PyStars version: %s', self._version)
        self._logger.info('Start server initialization.')
        self._logger.info('Port: %s  LibDir: %s', self._port, self._libdir)
        random.seed()
        self._system_loadcommandpermission()
        self._system_loadaliases()
        self._system_loadreconnecttablepermission()
        return self._initialized

    def shutdown(self):
        for s in self._readable:
            self._delnode(s)
            s.close()

    def writetologfile(self, msg, lvl='I'):
        if lvl is 'I':
            self._logger.info(msg)
        elif lvl is 'E':
            self._logger.error(msg)
        elif lvl is 'EX':
            self._logger.exception(msg)
        elif lvl is 'D':
            self._logger.debug(msg)


def setloglevel(lvl):
    if lvl == 'critical':
        return logging.CRITICAL
    if lvl == 'debug':
        return logging.DEBUG
    if lvl == 'error':
        return logging.ERROR
    return logging.INFO


def readconfigfile(cfile):
    cfg = configparser.ConfigParser(allow_no_value=True)
    cfg.read(cfile)
    starsport = cfg.getint("param", "starsport")
    starslib = cfg["param"]["starslib"]
    starskey = cfg["param"]["starskey"]
    starslogfile = cfg["param"]["starslogfile"]
    starsloglevel = setloglevel(cfg["param"]["starsloglevel"])
    starslogsize = cfg.getfloat("param", "starslogsize")
    starslogbackup = cfg.getint("param", "starslogbackup")
    return Stars(port=starsport, lib=starslib, key=starskey, logfile=starslogfile, loglevel=starsloglevel, logsize=starslogsize, logbackup=starslogbackup)

def readparameter():
    _parser = ArgumentParser()
    _parser.add_argument('-port', dest='p', type=int, default=DEFAULT_PORT)
    _parser.add_argument('-lib', dest='l', default=LIBDIR)
    _parser.add_argument('-key', dest='k', default=KEYDIR)
    _parser.add_argument('-logfile', dest='log', default=DEFAULT_LOGFILE)
    _parser.add_argument('-loglevel', dest='loglvl', default=DEFAULT_LOGLEVEL)
    _parser.add_argument('-logsize', dest='logs', default=DEFAULT_LOGSIZE)
    _parser.add_argument('-logbackup', dest='logback', default=DEFAULT_LOGBACKUP)
    args = _parser.parse_args()
    return Stars(port=args.p, lib=args.l, key=args.k, logfile=args.log, loglevel=args.loglvl, logsize=args.logs, logbackup=args.logback)

if __name__ == "__main__":
    print('\nSTARS Server Version: {}'.format(__version__))
    if not sys.version_info[:2] >= (3, 6):
        print('Python 3.6 or higher is required!')
        print(f'Detected version: {sys.version_info[:2]}')
        input('Press enter to exit...')
        sys.exit(0)

    cfgPath = StarsFile.getfilepath(os.path.dirname(os.path.realpath(__file__)), CONFIGFILE)
    if(cfgPath.is_file()):
        stars = readconfigfile(cfgPath)
    else:
        stars = readparameter()

    if stars.startup():
        print('Server started. Time:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        stars.writetologfile('Server initialization done')
        stars.writetologfile('Server start')
        stars.runserver()
    else:
        stars.writetologfile('Initialization failed! Server will not starting!', 'E')
        print('Initialization failed! Server will not starting! See server.log for more informations!')
        input('Press enter to exit...')
