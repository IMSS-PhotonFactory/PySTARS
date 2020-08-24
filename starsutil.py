""" STARS Server utility functions. """

import os
import random
import socket
import re
import time
import starsfile

TCP_BUFFER_SIZE = 512
RNDMAX = 10000
HOSTLIST = 'allow.cfg'
ALIASES = 'aliases.cfg'
CMDDENY = 'command_deny.cfg'
CMDALLOW = 'command_allow.cfg'
RECONNECTABLEDENY = 'reconnectable_deny.cfg'
RECONNECTABLEALLOW = 'reconnectable_allow.cfg'

def get_hostlist():
    return HOSTLIST

def get_tcpbuffersize():
    return TCP_BUFFER_SIZE

def get_nodeidkey():
    return random.randint(0, RNDMAX)

def system_gettime(tin=None):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(tin))

def system_gethostname_or_ip(sock, value='both'):
    hostname = ''
    ip, _unused = sock.getpeername()
    try:
        host = socket.gethostbyaddr(ip)
        hostname = host[0]
    except Exception as ex:
        hostname = ip
    if value == 'ip':
        return ip
    if value == 'host':
        return hostname
    if value == 'both':
        return hostname, ip
    return None

def system_checkhost(l, hostname, ipadr, unchecked, libdir):
    check = [hostname]
    if hostname != ipadr:
        check.append(ipadr)
    allowedhost = starsfile.loadfiletolist(l, os.path.dirname(os.path.realpath(__file__)), libdir)
    for entry in allowedhost:
        entry = re.sub(r"\.", r"\.", entry)
        entry = re.sub(r"\*", r".+", entry)
        p = re.compile(r"^" + entry + r"$")
        for c in check:
            if p.match(c):
                return True
    return unchecked

def check_term_and_host(nd, hd, libdir):
    if not starsfile.checkfileexist(nd + '.allow', os.path.dirname(os.path.realpath(__file__)), libdir):
        return True
    host, ip = system_gethostname_or_ip(hd)
    if system_checkhost('.allow', host, ip, True, libdir):
        return True
    return False

def check_nodekey(nname, nkeynum, nkeyval, keydir):
    kcount = 0
    if not starsfile.checkfileexist(nname + '.key', os.path.dirname(os.path.realpath(__file__)), keydir):
        return False
    kfile = starsfile.loadkeyfile(nname + '.key', os.path.dirname(os.path.realpath(__file__)), keydir)
    kcount = kfile.__len__()
    if kcount == 0:
        return False
    kcount = nkeynum % kcount
    if kfile[kcount] == nkeyval:
        return True
    return False

def isdenycheckcmd_deny(frm, to, buf, cmddeny):
    buf = re.search(r"^(\S+)( |$)", buf)
    if not buf:
        return True
    buf = "%s>%s %s" %(frm, to, buf.group())
    for chk in cmddeny:
        if re.findall(chk, buf):
            return True
    return False

def isdenycheckcmd_allow(frm, to, buf, cmdallow):
    buf = re.search(r"^(\S+)( |$)", buf)
    if not buf:
        return True
    buf = "%s>%s %s" %(frm, to, buf.group())
    for chk in cmdallow:
        if re.findall(chk, buf):
            return False
    return True

def isdennycheckreconnecttable_deny(node, host, reconndeny):
    for chk in reconndeny:
        if (re.match(r"^%s\s+%s$" %(node, host), chk)) or (re.match(r"^%s$" %node, chk)):
            return True
    return False

def isdennycheckreconnecttable_allow(node, host, reconnallow):
    for chk in reconnallow:
        if (re.match(r"^%s\s+%s$" %(node, host), chk)) or (re.match(r"^%s$" %node, chk)):
            return False
    return True

def system_listnodes(node):
    return " ".join(node)

def system_listaliases(aliasreal):
    return " ".join(f"{k},{v}" for k, v in aliasreal.items())

def check_reconnecttable(node, hd, reconndeny, reconnallow):
    if (not reconndeny) and (not reconnallow):
        return False
    if ((reconndeny) and (isdennycheckreconnecttable_deny(node, system_gethostname_or_ip(hd, 'host'), reconndeny)))\
        or ((reconnallow) and (isdennycheckreconnecttable_allow(node, system_gethostname_or_ip(hd, 'host'), reconnallow))):
        return False
    return True

def system_loadcommandpermission(libdir, cmddeny, cmdallow):
    try:
        cmddeny = starsfile.loadfiletolist(CMDDENY, os.path.dirname(os.path.realpath(__file__)), libdir)
        cmdallow = starsfile.loadfiletolist(CMDALLOW, os.path.dirname(os.path.realpath(__file__)), libdir)
        return True
    except Exception:
        return False

def system_loadaliases(libdir, aliasreal, realalias):
    try:
        starsfile.loadfiletodictionary(ALIASES, os.path.dirname(os.path.realpath(__file__)), libdir, aliasreal, realalias)
        return True
    except Exception:
        return False

def system_loadreconnecttablepermission(libdir, reconndeny, reconnallow):
    try:
        reconndeny = starsfile.loadfiletolist(RECONNECTABLEDENY, os.path.dirname(os.path.realpath(__file__)), libdir)
        reconnallow = starsfile.loadfiletolist(RECONNECTABLEALLOW, os.path.dirname(os.path.realpath(__file__)), libdir)
        return True
    except Exception:
        return False
