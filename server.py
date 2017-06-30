#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import os
import time
from threading import Thread, Lock
import uuid
import json
import zmq


class LockServer:
    def __init__(self, sid, cfg):
        self.sid = sid
        self.cfg = cfg
        self.uuid = uuid.uuid1()
        self.servers = []
        r = json.load(open(self.cfg))
        self.servers.append(r['leader'])
        self.servers += r['followers']
        self.ctx = zmq.Context()
        self.lock_table = {}
        self.mutex = Lock()
        self.status()

    def server_name(self, server_id):
        return 'leader' if server_id == 0 else 'follower'

    def status(self):
        print('+' * 16 + ' ' * 6 + 'Client Status' + ' ' * 6 + '+' * 16)
        print('time: {}'.format(time.ctime()))
        print('uuid: {}'.format(self.uuid))
        print('servers:')
        for idx, server in enumerate(self.servers):
            print('\t#{}{} {}: {}:{}'.format(idx, ' ' if idx != self.sid else '*', self.server_name(idx), server['ip'],
                                             server['port']))

        if len(self.lock_table):
            print('lock table:')
            for k, s in self.lock_table.items():
                print('\t{}: {}'.format(k, s))

        print('')

    def handle_client(self):
        skt = self.ctx.socket(zmq.REP)
        skt.bind('tcp://*:{}'.format(self.servers[self.sid]['port']))
        while True:
            req = skt.recv_json()
            rst = {
                'id': str(self.uuid),
                'key': req['key'],
                'status': 'ok'
            }
            if req['cmd'] == 'lock':
                if self.sid:
                    cmd_skt = self.ctx.socket(zmq.REQ)
                    cmd_skt.connect("tcp://{}:{}".format(self.servers[0]['ip'], self.servers[0]['port']))
                    cmd_skt.send_json(req)
                    skt.send_json(cmd_skt.recv_json())
                    continue
                else:
                    self.mutex.acquire()
                    if req['key'] in self.lock_table:
                        rst['status'] = 'failed'
                    else:
                        self.lock_table[req['key']] = req['uuid']
                        for follower in self.servers[1:]:
                            cmd_skt = self.ctx.socket(zmq.REQ)
                            cmd_skt.connect("tcp://{}:{}".format(follower['ip'], follower['cmd_port']))
                            cmd_skt.send_json(req)
                            cmd_skt.recv_json()
                    self.mutex.release()
            elif req['cmd'] == 'unlock':
                if self.sid:
                    cmd_skt = self.ctx.socket(zmq.REQ)
                    cmd_skt.connect("tcp://{}:{}".format(self.servers[0]['ip'], self.servers[0]['port']))
                    cmd_skt.send_json(req)
                    skt.send_json(cmd_skt.recv_json())
                    continue
                else:
                    self.mutex.acquire()
                    if (req['key'] not in self.lock_table or
                                req['uuid'] != self.lock_table[req['key']]):
                        rst['status'] = 'failed'
                    else:
                        del self.lock_table[req['key']]
                        for follower in self.servers[1:]:
                            cmd_skt = self.ctx.socket(zmq.REQ)
                            cmd_skt.connect("tcp://{}:{}".format(follower['ip'], follower['cmd_port']))
                            cmd_skt.send_json(req)
                            cmd_skt.recv_json()
                    self.mutex.release()
            elif req['cmd'] == 'check':
                self.mutex.acquire()
                if (req['key'] not in self.lock_table):
                    rst['status'] = 'failed'
                else:
                    rst['owner'] = self.lock_table[req['key']]
                self.mutex.release()
            skt.send_json(rst)

    def handle_server(self):
        skt = self.ctx.socket(zmq.REP)
        skt.bind('tcp://*:{}'.format(self.servers[self.sid]['cmd_port']))
        while True:
            req = skt.recv_json()
            rst = {
                'id': str(self.uuid),
                'key': req['key'],
                'status': 'ok'
            }
            if req['cmd'] == 'lock':
                self.mutex.acquire()
                self.lock_table[req['key']] = req['uuid']
                self.mutex.release()
            elif req['cmd'] == 'unlock':
                self.mutex.acquire()
                del self.lock_table[req['key']]
                self.mutex.release()
            skt.send_json(rst)

    def run(self):
        threads = []
        threads.append(Thread(target=self.handle_client))
        if self.sid:
            threads.append(Thread(target=self.handle_server))

        for t in threads: t.start()
        for t in threads: t.join()

    def lock(self, cmd):
        try:
            _, k, sid = cmd.split()
            sid = int(sid)
        except ValueError:
            print("usage: lock key server_id")
            return
        if sid < 0 or sid >= len(self.servers):
            print("{} is out of server id range [{}, {})".format(sid, 0, len(self.servers)))
            return

        skt = self.cxt.socket(zmq.REQ)
        skt.connect('tcp://{}:{}'.format(self.servers[sid]['ip'], self.servers[sid]['port']))
        skt.send_json({
            'uuid': str(self.uuid),
            'key': k
        })
        reply = skt.recv_json()
        if reply['status'] == 'ok':
            print('lock {} successfully.'.format(k))
        else:
            print('lock {} failed.'.format(k))


def parse_args():
    parser = argparse.ArgumentParser(description='Distributed Lock System (server)')
    parser.add_argument('--sid', help='server id', required=True, type=int)
    parser.add_argument('--server_cfg', help='server configuration file(json format)', default='server.cfg', type=str)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    if not os.path.exists(args.server_cfg):
        print('server configuration file {} not found!'.format(args.server_cfg))
        return
    server = LockServer(args.sid, args.server_cfg)
    server.run()


if __name__ == '__main__':
    main()
