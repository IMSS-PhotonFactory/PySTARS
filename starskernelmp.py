""" STARS Server multiprocessing module. """

import socket
import time
import re
import random
import threading
import multiprocessing as mp
import starsutil

TCP_BUFFER_SIZE = starsutil.get_tcpbuffersize()

class StarsMessage:
    def __init__(self, fromnode='', data=''):
        self._from = fromnode
        self._data = data

    def get_from(self):
        return self._from

    def get_data(self):
        return self._data

class SendRecvProcess(mp.Process):
    def __init__(self, nodename, sock, recv_q, send_q):
        super(SendRecvProcess, self).__init__()
        self._mynodename = nodename
        self._sock = sock
        self._recv_q = recv_q
        self._send_q = send_q
        self._run = True

    def get_nodename(self):
        return self._mynodename

    def get_socket(self):
        return self._sock

    def _recv_data(self, sock):
        datafragments = []
        try:
            while True:
                datapiece = sock.recv(TCP_BUFFER_SIZE).decode("utf8")
                datafragments.append(datapiece)
                if '\n' in datapiece:
                    break
                if len(datapiece) == 0:
                    break
        except:
            #Ignore the exception!
            pass
        return ''.join(datafragments)

    def close_connection(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()

    def _recvthread(self):
        savebuf = ''
        while True:
            data = self._recv_data(self._sock)
            if (len(savebuf) != 0):
                data = savebuf + data
                savebuf = ''
            if (len(data) != 0):
                m = re.split(r"\r*\n", data)
                if '' in m:
                    m.remove('')
                else:
                    savebuf = m[-1]
                    del m[-1]
                for buf in m:
                    if re.match(r"(?i)^(exit|quit)", buf):
                        self.close_connection()
                        self._run = False
                        break
                    else:
                        datamsg = StarsMessage(self._mynodename, buf)
                        self._recv_q.put(datamsg)
            else:
                self.close_connection()
                self._run = False

            if not self._run:
                break

    def _sendthread(self):
        while True:
            sendmsg = self._send_q.get(block=True)
            buf = sendmsg.get_data().encode()
            try:
                while len(buf) > 0:
                    send = self._sock.send(buf)
                    buf = buf[send:]
            except Exception:
                self.close_connection()
                break

    def run(self):
        recv_thread = threading.Thread(target=self._recvthread)
        recv_thread.daemon = True
        recv_thread.start()

        send_thread = threading.Thread(target=self._sendthread)
        send_thread.daemon = True
        send_thread.start()

        recv_thread.join()
        exit(13)
        send_thread.join()
        exit(13)


class Starsserver:
    def __init__(self, port, lib, key):
        self._port = port
        self._libdir = lib
        if (key is None) or (key == ''):
            self._keydir = lib
        else:
            self._keydir = key

        self._node = []
        self._node_flgon = {}

        self._cmddeny = []
        self._cmdallow = []
        self._reconndeny = []
        self._reconnallow = []
        self._aliasreal = {}
        self._realalias = {}
        self._initialized = True

        self._socket = None
        self._listener_thread = None
        self._msg_handle_thread = None
        self._proc_check_thread = None
        self._lock = threading.Lock()
        self._process_n = {}
        self._recv_q = mp.Queue()
        self._send_dict = {}

    def runserver(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(('', self._port))
        self._socket.listen()

        self._listener_thread = threading.Thread(target=self._listener, name='Listener')
        self._listener_thread.daemon = True
        self._listener_thread.start()

        self._msg_handle_thread = threading.Thread(target=self._msg_handler, name='MsgHandler')
        self._msg_handle_thread.daemon = True
        self._msg_handle_thread.start()

        self._proc_check_thread = threading.Thread(target=self._check_process, name='Check')
        self._proc_check_thread.daemon = True
        self._proc_check_thread.start()

        self._listener_thread.join()
        self._msg_handle_thread.join()
        self._proc_check_thread.join()

    def _sendconnmsg(self, xfh, xbuf):
        buf = xbuf.encode()
        try:
            while len(buf) > 0:
                send = xfh.send(buf)
                buf = buf[send:]
        except Exception:
            xfh.close()
        if 'Debugger' in self._node:
            dmsg = StarsMessage(None, xbuf)
            self._send_dict['Debugger'].put(dmsg)

    def _recvconnmsg(self, xfh):
        datafragments = []
        try:
            while True:
                datapiece = xfh.recv(TCP_BUFFER_SIZE).decode("utf8")
                if not datapiece:
                    break
                if '\n' in datapiece:
                    break
            datafragments.append(datapiece)
        except:
        #Ignore the exception!
            pass
        return ''.join(datafragments)

    def _puttosend(self, tonode, buf):
        sendmsg = StarsMessage(None, buf,)
        self._send_dict[tonode].put(sendmsg)
        if 'Debugger' in self._node:
            self._send_dict['Debugger'].put(sendmsg)

    def _listener(self):
        while True:
            new_sock, unused = self._socket.accept()
            bufhn, ipadr = starsutil.system_gethostname_or_ip(new_sock)
            if not starsutil.system_checkhost(starsutil.get_hostlist(), bufhn, ipadr, False, self._libdir):
                self._sendconnmsg(new_sock, "Bad host. %s\n" %bufhn)
                new_sock.close()
            else:
                nodekey = starsutil.get_nodeidkey()
                self._sendconnmsg(new_sock, "%s\n" %nodekey)
                data = self._recvconnmsg(new_sock)
                if (len(data) != 0):
                    add, node = self._addnode(new_sock, data.strip(), nodekey)
                    if add:
                        process = SendRecvProcess(node, new_sock, self._recv_q, self._send_dict[node])
                        process.daemon = True
                        process.start()
                        self._lock.acquire()
                        self._process_n[node] = process
                        self._lock.release()
                new_sock.close()

    def _check_process(self):
        while True:
            self._lock.acquire()
            for node in list(self._process_n):
                nProc = self._process_n[node]
                if nProc.exitcode == 13:
                    nProc.join()
            self._lock.release()
            time.sleep(0.5)
            self._lock.acquire()
            for node in list(self._process_n):
                nProc = self._process_n[node]
                if nProc.is_alive() == False:
                    if node in self._node:
                        self._delnode(node)
                    del self._send_dict[node]
                    del self._process_n[node]
            self._lock.release()

    def _msg_handler(self):
        while True:
            rmsg = self._recv_q.get(block=True)
            self._sendmes(rmsg)

    def _sendmes(self, frommsg):
        buf = frommsg.get_data()
        fromnode = fromnodes = sendh = frommsg.get_from()
        m = re.match(r"^([a-zA-Z_0-9.\-]+)>", buf)
        buf = re.sub(r"^([a-zA-Z_0-9.\-]+)>", '', buf, count=1)
        if m:
            fromnode = m.group(1)
        m = re.match(r"^([a-zA-Z_0-9.\-]+)\s*", buf)
        buf = re.sub(r"^([a-zA-Z_0-9.\-]+)\s*", '', buf, count=1)
        if not m:
            self._puttosend(sendh, "System>%s> @\n" %fromnode)
            return
        tonodes = m.group(1)
        if tonodes in self._aliasreal:
            tonodes = self._aliasreal[tonodes]
        if (re.match(r"^[^@]", buf)) and (((self._cmddeny) and (starsutil.isdenycheckcmd_deny(fromnodes, tonodes, buf, self._cmddeny)))\
            or ((self._cmdallow) and (starsutil.isdenycheckcmd_allow(fromnodes, tonodes, buf, self._cmdallow)))):
            if re.match(r"^[^_]", buf):
                self._puttosend(sendh, "System>%s @%s Er: Command denied.\n" %(fromnode, buf))
            return
        tonode = tonodes
        tonode = tonode.split('.', 1)[0]
        if tonode == 'System':
            return self._system_commands(sendh, fromnode, buf)
        if not tonode in self._node:
            if not re.match(r"^[_@]", buf):
                self._puttosend(sendh, "System>%s @%s Er: %s is down.\n" %(fromnode, buf, tonode))
            return
        if fromnode in self._realalias:
            fromnode = self._realalias[fromnode]
        self._puttosend(tonode, "%s>%s %s\n" %(fromnode, tonodes, buf))

    def _system_commands(self, sendh, frn, cmd):
        if cmd.startswith('_'):
            self._system_event(frn, cmd)
        elif re.match(r"disconnect ", cmd):
            cmd = cmd.replace('disconnect ', '')
            self._system_disconnect(sendh, frn, cmd)
        elif re.match(r"flgon ", cmd):
            cmd = cmd.replace('flgon ', '')
            self._system_flgon(sendh, frn, cmd)
        elif re.match(r"flgoff ", cmd):
            cmd = cmd.replace('flgoff ', '')
            self._system_flgoff(sendh, frn, cmd)
        elif cmd == 'loadreconnectablepermission':
            starsutil.system_loadreconnecttablepermission(self._libdir, self._reconndeny, self._reconnallow)
            self._puttosend(sendh, "System>%s @loadreconnectablepermission Reconnectable permission list has been loaded.\n" %frn)
        elif cmd == 'loadpermission':
            starsutil.system_loadcommandpermission(self._libdir, self._cmddeny, self._cmdallow)
            self._puttosend(sendh, "System>%s @loadpermission Command permission list has been loaded.\n" %frn)
        elif cmd == 'loadaliases':
            starsutil.system_loadaliases(self._libdir, self._aliasreal, self._realalias)
            self._puttosend(sendh, "System>%s @loadaliases Aliases has been loaded.\n" %frn)
        elif cmd == 'listaliases':
            self._puttosend(sendh, "System>%s @listaliases %s\n" %(frn, starsutil.system_listaliases(self._aliasreal)))
        elif cmd == 'listnodes':
            self._puttosend(sendh, "System>%s @listnodes %s\n" %(frn, starsutil.system_listnodes(self._node)))
        elif cmd == 'gettime':
            self._puttosend(sendh, "System>%s @gettime %s\n" %(frn, starsutil.system_gettime()))
        elif cmd == 'hello':
            self._puttosend(sendh, "System>%s @hello Nice to meet you.\n" %frn)
        elif cmd == 'help':
            self._puttosend(sendh, "System>%s @help flgon flgoff loadaliases listaliases loadpermission loadreconnectablepermission listnodes gettime hello disconnect\n" %frn)
        elif cmd.startswith('@'):
            return True
        else:
            self._puttosend(sendh, "System>%s @%s Er: Command is not found or parameter is not enough.\n" %(frn, cmd))
        return True

    def _system_event(self, frn, cmd):
        to = None
        if frn in self._realalias:
            frn = self._realalias[frn]
        for key in self._node_flgon.keys():
            if re.search(frn, self._node_flgon[key]):
                to = topre = key
                topre = topre.split('.', 1)[0]
                self._puttosend(topre, "%s>%s %s\n" %(frn, to, cmd))

    def _addnode(self, sendh, buff, nodekey):
        try:
            node, idmess = buff.split(' ')
        except Exception:
            return False, None
        reconnectflag = False
        if node in self._node:
            if not starsutil.check_reconnecttable(node, sendh, self._reconndeny, self._reconnallow):
                self._sendconnmsg(sendh, "System> Er: %s already exists.\n" %node)
                return False, node
            else:
                reconnectflag = True
        if not starsutil.check_term_and_host(node, sendh, self._libdir):
            self._sendconnmsg(sendh, "System> Er: Bad host for %s\n" %node)
            return False, node
        if not starsutil.check_nodekey(node, nodekey, idmess, self._keydir):
            self._sendconnmsg(sendh, "System> Er: Bad node name or key\n")
            return False, node
        if reconnectflag:
            self._disconnect_for_reconnect(node)
        self._node.append(node)
        self._send_dict[node] = mp.Queue()
        self._sendconnmsg(sendh, "System>%s Ok:\n" %node)
        if node in self._realalias:
            node = self._realalias[node]
        for key in self._node_flgon.keys():
            if re.search(node, self._node_flgon[key]):
                topre = key
                topre = topre.split('.', 1)[0]
                self._puttosend(topre, "%s>%s _Connected\n" %(node, key))
        return True, node

    def _delnode(self, node):
        try:
            topre = None
            if node not in self._node:
                return
            self._node.remove(node)
            for key in list(self._node_flgon.keys()):
                if re.findall(r"%s($|.)" %node, key):
                    del self._node_flgon[key]
            if node in self._realalias:
                node = self._realalias[node]
            for key in self._node_flgon.keys():
                if re.search(node, self._node_flgon[key]):
                    topre = key
                    topre = topre.split('.', 1)[0]
                    self._puttosend(topre, "%s>%s _Disconnected\n" %(node, key))
        except Exception as ex:
            print('Exception occurred: ', ex)

    def _system_disconnect(self, sendh, frn, cmd):
        if not re.match(r"^([a-zA-Z_0-9.\-]+)", cmd):
            self._puttosend(sendh, "System>%s @disconnect Er: Parameter is not enough.\n" %frn)
            return False
        if cmd in self._aliasreal:
            cmd = self._aliasreal[cmd]
        if not cmd in self._node:
            self._puttosend(sendh, "System>%s @disconnect Er: Node %s is down.\n" %(frn, cmd))
            return False
        self._puttosend(sendh, "System>%s @disconnect %s.\n" %(frn, cmd))
        self._delnode(cmd)
        del self._send_dict[cmd]
        self._lock.acquire()
        self._process_n[cmd].close_connection()
        del self._process_n[cmd]
        self._lock.release()
        return True

    def _system_flgon(self, sendh, frn, cmd):
        if not re.match(r"^([a-zA-Z_0-9.\-]+)", cmd):
            self._puttosend(sendh, "System>%s @flgon Er: Parameter is not enough.\n" %frn)
            return False
        if frn in self._node_flgon:
            if (re.findall(cmd, self._node_flgon[frn])):
                self._puttosend(sendh, "System>%s @flgon Er: Node %s is allready in the list.\n" %(frn, cmd))
                return False
            self._node_flgon[frn] += ' ' + cmd
            self._puttosend(sendh, "System>%s @flgon Node %s has been registered.\n" %(frn, cmd))
            return True
        self._node_flgon[frn] = cmd
        self._puttosend(sendh, "System>%s @flgon Node %s has been registered.\n" %(frn, cmd))
        return True

    def _system_flgoff(self, sendh, frn, cmd):
        if not re.match(r"^([a-zA-Z_0-9.\-]+)", cmd):
            self._puttosend(sendh, "System>%s @flgoff Er: Parameter is not enough.\n" %frn)
            return False
        if frn in self._node_flgon:
            if (re.findall(cmd, self._node_flgon[frn])):
                self._node_flgon[frn] = self._node_flgon[frn].replace(cmd, '').lstrip()
                self._puttosend(sendh, "System>%s @flgoff Node %s has been removed.\n" %(frn, cmd))
                return True
            else:
                self._puttosend(sendh, "System>%s @flgoff Er: Node %s is not in the list.\n" %(frn, cmd))
                return False
        else:
            self._puttosend(sendh, "System>%s @flgoff Er: List is void.\n" %frn)
            return False

    def _disconnect_for_reconnect(self, node):
        self._delnode(node)
        del self._send_dict[node]
        self._lock.acquire()
        self._process_n[node].terminate()
        self._process_n[node].join()
        del self._process_n[node]
        self._lock.release()

    def startup(self):
        _initialized = True
        random.seed()
        _initialized = starsutil.system_loadcommandpermission(self._libdir, self._cmddeny, self._cmdallow)
        _initialized = starsutil.system_loadaliases(self._libdir, self._aliasreal, self._realalias)
        _initialized = starsutil.system_loadreconnecttablepermission(self._libdir, self._reconndeny, self._reconnallow)
        return _initialized
