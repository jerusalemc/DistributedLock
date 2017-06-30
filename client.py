#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import os
import time
import uuid
import json
import zmq
import multiprocessing


class LockClient:
    def __init__(self, cid, cfg):
        self.cid = cid
        self.cfg = cfg
        self.uuid = uuid.uuid1()
        self.servers = []
        r = json.load(open(self.cfg))
        self.servers.append(r['leader'])
        self.servers += r['followers']
        self.ctx = zmq.Context()
        self.lock_table = {}
        self.status()

    def server_name(self, server_id):
        return 'leader' if server_id == 0 else 'follower'

    def status(self):
        print('+' * 16 + ' ' * 6 + 'Client Status' + ' ' * 6 + '+' * 16)
        print('time: {}'.format(time.ctime()))
        print('uuid: {}'.format(self.uuid))
        print('cid: {}'.format(self.cid))

        # print servers info.
        print('servers:')
        for idx, server in enumerate(self.servers):
            print('\t#{} {}: {}:{}'.format(idx, self.server_name(idx), server['ip'], server['port']))
        print()

        # print lock table
        if len(self.lock_table):
            print('lock table')
            for k, s in self.lock_table.items():
                print('\t{}: {}'.format(k, s))

    def run(self):
        while(True):
            cmd = raw_input('>>> ')
            cmd = cmd.lower()
            if cmd == 'status':
                self.status()
            elif cmd == 'exit':
                break
            elif cmd.startswith('lock'):
                self.lock(self.send_cmd('lock', cmd))
            elif cmd.startswith('unlock'):
                self.unlock(self.send_cmd('unlock', cmd))
            elif cmd.startswith('check'):
                self.check(self.send_cmd('check', cmd))
            else:
                print('{}: unknown commands.'.format(cmd))

    def run_from_file(self):
        file_name = 'client0' + str(self.cid) + '.txt'
        with open(file_name) as f:
            cmds = f.readlines()
            for item in cmds:
                cmd = item.split('\n')[0]
                if cmd == 'status':
                    self.status()
                elif cmd == 'exit':
                    break
                elif cmd.startswith('lock'):
                    self.lock(self.send_cmd('lock', cmd))
                elif cmd.startswith('unlock'):
                    self.unlock(self.send_cmd('unlock', cmd))
                elif cmd.startswith('check'):
                    self.check(self.send_cmd('check', cmd))
                else:
                    print('{}: unknown commands.'.format(cmd))
                time.sleep(10)




    def send_cmd(self, cmd_name, cmd):
        try:
            _, k , sid = cmd.split()
            sid = int(sid)
        except ValueError:
            print("usage: {} key server_id".format(cmd_name))
            return
        if sid < 0 or sid >= len(self.servers):
            print("{} is out of server id range [{}, {})".format(sid, 0, len(self.servers)))
            return

        skt = self.ctx.socket(zmq.REQ)
        skt_name = 'tcp://{}:{}'.format(self.servers[sid]['ip'], self.servers[sid]['port'])
        print('connecting to {}...'.format(skt_name))
        skt.connect(skt_name)
        skt.send_json({
                        'cmd': cmd_name,
                        'uuid': str(self.cid),
                        'key': k
                      })
        reply = skt.recv_json()
        return reply

    def lock(self, reply):
        if reply is None: return
        if reply['status'] == 'ok':
            self.lock_table[reply['key']] = 'owned'
            print('lock {} successfully by client{}.'.format(reply['key'], self.cid))
        else:
            print('lock {} failed by client{}.'.format(reply['key'], self.cid))

    def unlock(self, reply):
        if reply is None: return
        if reply['status'] == 'ok':
            if reply['key'] in self.lock_table: del self.lock_table[reply['key']]
            print('unlock {} successfully by client{}.'.format(reply['key'], self.cid))
        else:
            print('unlock {} failed by client{}.'.format(reply['key'], self.cid))

    def check(self, reply):
        if reply is None: return
        if reply['status'] == 'ok':
            print('The owner of {} is client{}, check by client{}.'.format(reply['key'], reply['owner'], self.cid))
        else:
            print('check owner for {} is failed by client{}.'.format(reply['key'], self.cid))


def parse_args():
    parser = argparse.ArgumentParser(description='Distributed Lock System (Client)')
    parser.add_argument('--cid', help='client id', required=True, type=int)
    parser.add_argument('--server_cfg', help='server configuration file(json format)', default='server.cfg', type=str)
    args = parser.parse_args()
    return args


#def main(cid):
def main():
    args = parse_args()
    if not os.path.exists(args.server_cfg):
        print('server configuration file {} not found!'.format(args.server_cfg))
        return
    #client = LockClient(cid, args.server_cfg)
    
    client = LockClient(args.cid, args.server_cfg)
    # print(args.cid)
    client.run()
    #client.run_from_file()

if __name__ == '__main__':
   #p1 = multiprocessing.Process(target=main, args=(1,))
   #p2 = multiprocessing.Process(target=main, args=(2,))
   #p3 = multiprocessing.Process(target=main, args=(3,))
   #p1.start()
   #time.sleep(3)
   #p2.start()
   #time.sleep(3)
   #p3.start()

   main()
