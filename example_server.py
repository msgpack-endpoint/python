# -*- coding: utf-8 -*-
from gevent.server import StreamServer
import gevent
from endpoint import *
import logging

class ExampleServerHandle(object):
    def __init__(self):
        self.callloop = None

    def echo(self, hello):
        return "echo from server: {}".format(hello), None

    def close(self):
        print("conn closed.")
        if self.callloop:
            self.callloop.kill()

def handle(skt, addr):
    print("new msgpack endpoint from {}".format(addr))
    router = RpcRouter()
    handler = ExampleServerHandle()
    router.route_call(handler.echo)
    print(router.get_calls())
    ep = ServerMsgpackEndpoint(MODEBOTH, skt, router)
    handler.callloop = gevent.spawn(callloop, ep)
    handler.callloop.start()
    err = ep.serve()
    print("endpoint %s closed", addr)
    handler.close()
    ep.close()
    if gevent.__version__ < "1.1.0":
        skt.close()

def callloop(ep):
    gevent.sleep(10.0)
    while True:
        print(ep.call("echo", "loop from server"))
        gevent.sleep(3.0)

if __name__ == "__main__":
    server = StreamServer(('0.0.0.0', 11000), handle)
    server.serve_forever()
