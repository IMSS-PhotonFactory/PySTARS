""" STARS Server single thread module. """
import select
import socket
import re
import random
from PyStars import __version__, __date__
import starsutil

TCP_BUFFER_SIZE = starsutil.get_tcpbuffersize()

class Starsserver:
    def __init__(self, port, lib, key):
        self._port = port
        self._libdir = lib
        if (key is None) or (key == ''):
            self._keydir = lib
        else:
            self._keydir = key

        self._node = {}
        self._node_h = {}
        self._node_flgon = {}
        self._node_idkey = {}
        self._aliasreal = {}
        self._realalias = {}

        self._savebuf = {}
        self._sockettoclose = []
        self._readable = []
        self._writeable = []
        self._writebuf = {}
        self._cmddeny = []
        self._cmdallow = []
        self._reconndeny = []
        self._reconnallow = []

    def runserver(self):
        try:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setblocking(0)
            listener.bind(('', self._port))
            listener.listen()
        except Exception as ex:
            print('Can\'t create socket for listining! ', ex)
            return False
        self._readable.append(listener)
        while True:
            read, write, _error_unused = select.select(self._readable, self._writeable, [], 2)
            for s in read:
                if s is listener:
                    new_sock, _unused = s.accept()
                    new_sock.setblocking(0)
                    self._readable.append(new_sock)
                    bufhn, ipadr = starsutil.system_gethostname_or_ip(new_sock)
                    if not starsutil.system_checkhost(starsutil.get_hostlist(), bufhn, ipadr, False, self._libdir):
                        self._add_to_send(new_sock, "Bad host. %s\n" %bufhn)
                        self._readable.remove(new_sock)
                        self._sockettoclose.append(new_sock)
                        continue
                    self._savebuf[new_sock] = ''
                    self._node_idkey[new_sock] = starsutil.get_nodeidkey()
                    self._add_to_send(new_sock, "%s\n" %self._node_idkey[new_sock])
                else:
                    #To handle large data
                    datafragments = []
                    try:
                        while True:
                            datapiece = s.recv(TCP_BUFFER_SIZE).decode("utf8")
                            if not datapiece:
                                break
                            datafragments.append(datapiece)
                    except:
                        #Ignore the exception!
                        pass
                    data = ''.join(datafragments)
                    if (len(self._savebuf[s]) != 0):
                        data = self._savebuf[s] + data
                        self._savebuf[s] = ''
                    if (len(data) != 0):
                        m = re.split(r"\r*\n", data)
                        if '' in m:
                            m.remove('')
                        else:
                            self._savebuf[s] = m[-1]
                            del m[-1]
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
                                    del self._node_idkey[s]
                                    self._readable.remove(s)
                                    if s in self._writeable:
                                        self._writeable.remove(s)
                                        del self._writebuf[s]
                                    s.close()
                    else:
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
                    for sock in self._sockettoclose:
                        sock.close()
                    self._sockettoclose.clear()

    def _add_to_send(self, xfh, xbuf):
        if xfh not in self._writeable:
            self._writeable.append(xfh)
            self._writebuf[xfh] = ''
        self._writebuf[xfh] += xbuf
        if 'Debugger' in self._node:
            handle = self._node['Debugger']
            if handle not in self._writeable:
                self._writeable.append(handle)
                self._writebuf[handle] = ''
            self._writebuf[handle] += xbuf

    def _printh(self, xfh):
        try:
            buf = (self._writebuf[xfh]).encode()
            if len(buf) == 0:
                return True
            send = xfh.send(buf)
            self._writebuf[xfh] = (buf[send:]).decode()
            return False
        except Exception:
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
            return
        tonodes = m.group(1)
        if tonodes in self._aliasreal:
            tonodes = self._aliasreal[tonodes]
        if (re.match(r"^[^@]", buf)) and (((self._cmddeny) and (starsutil.isdenycheckcmd_deny(fromnodes, tonodes, buf, self._cmddeny)))\
            or ((self._cmdallow) and (starsutil.isdenycheckcmd_allow(fromnodes, tonodes, buf, self._cmdallow)))):
            if re.match(r"^[^_]", buf):
                self._add_to_send(handle, "System>%s @%s Er: Command denied.\n" %(fromnode, buf))
            return
        tonode = tonodes
        tonode = tonode.split('.', 1)[0]
        if tonode == 'System':
            return self._system_commands(handle, fromnode, buf)
        if not tonode in self._node:
            if not re.match(r"^[_@]", buf):
                self._add_to_send(handle, "System>%s @%s Er: %s is down.\n" %(fromnode, buf, tonode))
            return
        if fromnode in self._realalias:
            fromnode = self._realalias[fromnode]
        tonodeh = self._node[tonode]
        self._add_to_send(tonodeh, "%s>%s %s\n" %(fromnode, tonodes, buf))

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
            starsutil.system_loadreconnecttablepermission(self._libdir, self._reconndeny, self._reconnallow)
            self._add_to_send(hd, "System>%s @loadreconnectablepermission Reconnectable permission list has been loaded.\n" %frn)
        elif cmd == 'loadpermission':
            starsutil.system_loadcommandpermission(self._libdir, self._cmddeny, self._cmdallow)
            self._add_to_send(hd, "System>%s @loadpermission Command permission list has been loaded.\n" %frn)
        elif cmd == 'loadaliases':
            starsutil.system_loadaliases(self._libdir, self._aliasreal, self._realalias)
            self._add_to_send(hd, "System>%s @loadaliases Aliases has been loaded.\n" %frn)
        elif cmd == 'listaliases':
            self._add_to_send(hd, "System>%s @listaliases %s\n" %(frn, starsutil.system_listaliases(self._aliasreal)))
        elif cmd == 'listnodes':
            self._add_to_send(hd, "System>%s @listnodes %s\n" %(frn, starsutil.system_listnodes(self._node)))
        elif cmd == 'getversion':
            self._add_to_send(hd, "System>%s @getversion Version: %s Date: %s\n" %(frn, __version__, __date__))
        elif cmd == 'gettime':
            self._add_to_send(hd, "System>%s @gettime %s\n" %(frn, starsutil.system_gettime()))
        elif cmd == 'hello':
            self._add_to_send(hd, "System>%s @hello Nice to meet you.\n" %frn)
        elif cmd == 'help':
            self._add_to_send(hd, "System>%s @help flgon flgoff loadaliases listaliases loadpermission loadreconnectablepermission listnodes getversion gettime hello disconnect\n" %frn)
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
            if re.search(frn, self._node_flgon[key]):
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
            return False
        reconnectflag = False
        if node in self._node:
            if not starsutil.check_reconnecttable(node, handle, self._reconndeny, self._reconnallow):
                self._add_to_send(handle, "System> Er: %s already exists.\n" %node)
                return False
            else:
                reconnectflag = True
        if not starsutil.check_term_and_host(node, handle, self._libdir):
            self._add_to_send(handle, "System> Er: Bad host for %s\n" %node)
            return False
        if not starsutil.check_nodekey(node, self._node_idkey[handle], idmess, self._keydir):
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
            if re.search(node, self._node_flgon[key]):
                topre = key
                topre = topre.split('.', 1)[0]
                buffh = self._node[topre]
                self._add_to_send(buffh, "%s>%s _Connected\n" %(node, key))
        return True

    def _delnode(self, handle):
        try:
            topre = None
            node = self._node_h.get(handle)
            if node is None:
                return
            del self._node_h[handle]
            del self._node[node]
            del self._node_idkey[handle]
            for key in list(self._node_flgon.keys()):
                if re.findall(r"%s($|.)" %node, key):
                    del self._node_flgon[key]
            if node in self._realalias:
                node = self._realalias[node]
            for key in self._node_flgon:
                if re.search(node, self._node_flgon[key]):
                    topre = key
                    topre = topre.split('.', 1)[0]
                    buffh = self._node[topre]
                    self._add_to_send(buffh, "%s>%s _Disconnected\n" %(node, key))
        except Exception as ex:
            print('Exception occurred: ', ex)

    def startup(self):
        _initialized = True
        random.seed()
        _initialized = starsutil.system_loadcommandpermission(self._libdir, self._cmddeny, self._cmdallow)
        _initialized = starsutil.system_loadaliases(self._libdir, self._aliasreal, self._realalias)
        _initialized = starsutil.system_loadreconnecttablepermission(self._libdir, self._reconndeny, self._reconnallow)
        return _initialized
