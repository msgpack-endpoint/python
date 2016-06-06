# -*- coding: utf-8 -*-
import msgpack
import gevent
from gevent import socket
from gevent.event import Event
from gevent.queue import Queue
import logging

MSGPACKRPC_REQ = 0
MSGPACKRPC_RSP = 1
MSGPACKRPC_NOTIFY = 2

MODECLIENT = 1
MODESERVER = 2
MODEBOTH = 3

class RpcRouter(object):
    def __init__(self):
        self._notify_funcs = {}
        self._call_funcs = {}

    def route_notify(self, func, name=None):
        if name == None:
            name = func.__name__
        self._notify_funcs[name] = func

    def route_call(self, func, name=None):
        if name == None:
            name = func.__name__
        self._call_funcs[name] = func

    def get_notify(self, name):
        return self._notify_funcs.get(name)

    def get_call(self, name):
        return self._call_funcs.get(name)

    def get_calls(self):
        return self._call_funcs.keys()

    def get_notifies(self):
        return self._notify_funcs.keys()

#do not use this class directly
class msgpackEndpoint(object):
    def __init__(self, mode, conn, router=None, timeout=5.0, poolsize=10, chunksize=1024*32, pack_encoding='utf-8', unpack_encoding='utf-8'):
        self._run = True
        self.error = None
        self._remote_addr = conn.getpeername()
        self._me = "Endpoint({}:{})".format(self._remote_addr[0], self._remote_addr[1])
        if router and type(router) != RpcRouter:
            logging.error("%s router type error:%s", self._me, router)
        self._router = router
        self._mode = mode
        self._timeout = timeout
        self._poolsize = poolsize
        self._msgpool = dict()
        self._pack_encoding = pack_encoding
        self._unpack_encoding = unpack_encoding
        self._packer = msgpack.Packer(use_bin_type=True, encoding=pack_encoding.encode("utf-8"))
        self._unpacker = msgpack.Unpacker(encoding=unpack_encoding.encode("utf-8"))
        self._conn = conn
        self._conn.settimeout(1.0)
        self._chunksize = chunksize
        self._connecting = False
        self._msgid = 0

    def serve(self):
        try:
            while self._run:
                try:
                    data = self._conn.recv(self._chunksize)
                except socket.timeout as t:
                    continue
                except Exception as e:
                    logging.exception("%s conn exception, %s", self._me, e)
                    self.error = e
                    self._run = False
                    return e
                if not data:
                    logging.info("%s conn closed", self._me)
                    self._run = False
                    return self.error
                self._unpacker.feed(data)
                while self._run:
                    try:
                        msg = self._unpacker.next()
                    except StopIteration as e:
                        break
                    err = self._parse_msg(msg)
                    if err:
                        self.error = err
                        self._run = False
                        break
            return self.error
        except Exception as e:
            logging.exception("%s serve unexpected exception: %s", self._me, e)
            logging.error("%s serve exit exception: %s", self._me, e)
            self.error = e
            self._run = False
            return self.error

    def _send(self, body):
        try:
            self._conn.sendall(body)
            return None
        except Exception as e:
            logging.warning("%s sending exception. %s", self._me, e)
            self.error = e
            self._run = False
            return e

    def _parse_msg(self, msg):
        if (type(msg) != list and type(msg) != tuple) or len(msg) < 3:
            logging.warn("{} invalid msgpack-rpc msg. type={}, msg={}".format(self._me, type(msg), msg))
            return Exception("{} invalid msg".format(self._me))
        if msg[0] == MSGPACKRPC_RSP and len(msg) == 4 and self._mode & MODECLIENT:
            (_, msgid, error, result) = msg
            if msgid not in self._msgpool:
                logging.warn("{} unexpected msgid. msgid = {}".format(self._me, msgid))
                return None
            msgsit = self._msgpool[msgid]
            del self._msgpool[msgid]
            msgsit[1] = error
            msgsit[2] = result
            msgsit[0].set()
            return None
        elif msg[0] == MSGPACKRPC_REQ and len(msg) == 4 and self._mode & MODESERVER:
            (_, msgid, method, params) = msg
            func = self._router.get_call(method)
            result = None
            if not func:
                rsp = (MSGPACKRPC_RSP, msgid, "Method not found: {}".format(method), None)
                return self._send(self._packer.pack(rsp))
            if not hasattr(func, '__call__'):
                rsp = (MSGPACKRPC_RSP, msgid, "Method is not callable: {}".format(method), None)
                return self._send(self._packer.pack(rsp))
            try:
                result, err = func(*params)
            except Exception as e:
                logging.exception("%s call of method %s Exception:%s", self._me, method, e)
                rsp = (MSGPACKRPC_RSP, msgid, "Internal Error", None)
                return self._send(self._packer.pack(rsp))
            rsp = (MSGPACKRPC_RSP, msgid, err.__str__() if err else None, result)
            return self._send(self._packer.pack(rsp))
        elif msg[0] == MSGPACKRPC_NOTIFY and len(msg) == 3 and self._mode & MODESERVER:
            (_, method, params) = msg
            func = self._router.get_notify(method)
            if not func:
                logging.warn("{} Method not found: {}".format(self._me, method))
                return None
            if not hasattr(func, '__call__'):
                logging.warn("{} Method is not callable: {}".format(self._me, method))
                return None
            try:
                func(*params)
            except Exception as e:
                logging.exception("%s notify %s exception: %s", self._me, method, e)
                return None
        else:
            logging.warn("{} invalid msgpack-rpc msg {}".format(self._me, msg))
            return Exception("invalid msgpack-rpc msg")
                    
    def call(self, method, *args):
        if not self._conn:
            logging.warn("%s rpc connection closed", self._me)
        if type(method) != str or type(args) != tuple:
            raise Exception("invalid msgpack-rpc request, type(method)={}, type(args)={}".format(type(method), type(args)))
        self._msgid += 1
        msgid = self._msgid
        req = (MSGPACKRPC_REQ, msgid, method, args)
        body = self._packer.pack(req)
        msgsit = [Event(), None, None]
        self._msgpool[msgid] = msgsit
        err = self._send(body)
        if err:
            del self._msgpool[msgid]
            raise err
        r = msgsit[0].wait(timeout=self._timeout)
        if not r:
            raise Exception("msgpack-rpc call timeout after {} seconds, msgid = {}".format(self._timeout, msgid))
        if msgsit[1]:
            raise Exception("msgpack-rpc call rsp error. {}".format(msgsit[1]))
        return msgsit[2]

    def notify(self, method, *args):
        if not self._conn:
            logging.warn("rpc connection closed")
        if type(method) != str or type(args) != tuple:
            raise Exception("invalid msgpack-rpc request, type(method)={}, type(args)={}".format(type(method), type(args)))
        notify = (MSGPACKRPC_NOTIFY, method, args)
        err = self._send(self._packer.pack(notify))
        if err:
            logging.warn("%s send notify error. err=%s", self._me, err)

    def close(self):
        self._run = False

class ServerMsgpackEndpoint(object):
    def __init__(self, mode, conn, router=None, timeout=5.0, poolsize=10, chunksize=1024*32, pack_encoding='utf-8', unpack_encoding='utf-8'):
        self._ep = msgpackEndpoint(mode, conn, router, timeout, poolsize, chunksize, pack_encoding, unpack_encoding)

    def close(self):
        self._ep.close()

    def call(self, method, *args):
        return self._ep.call(method, *args)

    def serve(self):
        return self._ep.serve()

    def notify(self, method, *args):
        self._ep.notify(method, *args)

class ClientMsgpackEndpoint(object):
    def __init__(self, mode, addr, router=None, timeout=5.0, poolsize=10, chunksize=1024*32, 
            pack_encoding='utf-8', unpack_encoding='utf-8', handle_after_connect=None, handle_before_connect=None):
        self._hdl_after_connect = handle_after_connect
        self._hdl_before_connect = handle_before_connect
        self._mode = mode
        self._addr = addr
        self._router = router
        self._timeout = timeout
        self._poolsize = poolsize
        self._chunksize = chunksize
        self._pack_encoding = pack_encoding
        self._unpack_encoding = unpack_encoding
        self._ep = None
        self._continus_connect_fail = 0
        self._serve_worker = gevent.spawn(self._serve)
        self._serve_worker.start()
        self._run = True
        self._error = None
        self._me = "ClientMsgpackEndpoint({})".format(self._addr)
        self._ep_watcher = gevent.spawn(self._ep_watch)
        self._ep_watcher.start()

    def _reset(self):
        if self._ep:
            self._ep.close()
            self._ep = None
        if self._conn:
            self._conn.close()
            self._conn = None

    def _newendpoint(self):
        try:
            self._conn = socket.create_connection(self._addr, timeout=self._timeout)
        except Exception as e:
            logging.info("%s connect failed. err=%s", self._me, e)
            return False
        self._ep = msgpackEndpoint(self._mode, self._conn, self._router, self._timeout, 
                self._poolsize, self._chunksize, self._pack_encoding, self._unpack_encoding)
        return True

    def _ep_watch(self):
        try:
            while self._run:
                if self._ep:
                    gevent.sleep(1.0)
                    continue
                if self._hdl_before_connect:
                    try:
                        succ = self._hdl_before_connect(self)
                    except Exception as e:
                        logging.exception("%s call _hdl_before_connect exception: %s", self._me, e)
                        succ = False
                    if not succ:
                        gevent.sleep(1.0)
                        continue
                if not self._run:
                    continue
                if not self._newendpoint():
                    self._continus_connect_fail += 1
                    gevent.sleep(1.0)
                    continue
                self._continus_connect_fail = 0
                if self._hdl_after_connect:
                    try:
                        succ = self._hdl_after_connect(self)
                    except Exception as e:
                        logging.exception("%s call _hdl_after_connect exception: %s", self._me, e)
                        succ = False
                    if not succ:
                        self._reset()
                        gevent.sleep(1.0)
                        continue
        except Exception as e:
            logging.exception("%s _ep_watch unexpected exception: %s", self._me, e)
            logging.error("%s exit exception: %s", self._me, e)
            self._error = e
            self._reset()
            self._run = False
        
    def _serve(self):
        try:
            while self._run:
                if not self._ep:
                    gevent.sleep(0.5)
                    continue
                self._error = None
                self._error = self._ep.serve()
                logging.info("%s serve return. err=%s", self._me, self._error)
                self._reset()
        except Exception as e:
            logging.exception("%s serve unexpected exception: %s", self._me, e)
            logging.error("%s serve exit exception: %s", self._me, e)
            self._reset()
            self._error = e
            self._run = False

    def error(self):
        return self._error

    def remote_addr(self):
        return self._addr

    def close(self):
        logging.info("%s closed", self._me)
        self._run = False
        self._reset()

    def is_run(self):
        return self._run

    def is_ready(self):
        return self._ep != None

    def continus_connect_fail(self):
        return self._continus_connect_fail

    def call(self, method, *args):
        if not self._ep:
            raise Exception("connection to {} closed. reconnecting...".format(self._addr))
        return self._ep.call(method, *args)

    def notify(self, method, *args):
        if not self._ep:
            raise Exception("connection to {} closed. reconnecting...".format(self._addr))
        self._ep.notify(method, *args)

