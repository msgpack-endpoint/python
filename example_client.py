# -*- coding: utf-8 -*-

from endpoint import *
import gevent
from gevent import socket
from rpchandle import *

def timer_call(ep):
    while True:
        if not ep.is_ready():
            gevent.sleep(1.0)
            continue
        try:
            print(ep.call("echo", "call from client"))
        except Exception as e:
            print("call failed. {}".format(e))
        gevent.sleep(3.0)

if __name__ == "__main__":
    sh = RPCHandle("client")
    router = RpcRouter()
    router.route_call(sh.echo)
    print(router.get_calls())
    ep = ClientMsgpackEndpoint(MODEBOTH, ("127.0.0.1", 11000), router)
    gevent.spawn(timer_call, ep).start()
    gevent.sleep(100000.0)
