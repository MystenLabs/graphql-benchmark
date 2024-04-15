# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

import base58
import binascii
import argparse

def digest_to_hex(digest):
    decoded = base58.b58decode(digest)
    return "'\\x" + binascii.hexlify(decoded).decode('utf-8') + "'"


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert a base58 digest to hex')
    parser.add_argument('--digest', type=str, help='The base58 digest to convert')
    args = parser.parse_args()

    print(digest_to_hex(args.digest))
