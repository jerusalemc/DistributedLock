#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import os
import subprocess
import json


def parse_args():
    parser = argparse.ArgumentParser(description='Distributed Lock System (Client)')
    parser.add_argument('--server_cfg', help='server configuration file(json format)', default='server.cfg', type=str)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    if not os.path.exists(args.server_cfg):
        print('server configuration file {} not found!'.format(args.server_cfg))
        return
    cfg = json.load(open(args.server_cfg))

    servers = []
    for idx in range(1 + len(cfg['followers'])):
        servers.append(subprocess.Popen(["./server.py", "--sid", str(idx)]))

    try:
        for s in servers: s.wait()
    except KeyboardInterrupt:
        for s in servers: s.terminate()


if __name__ == '__main__':
    main()
