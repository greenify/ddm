#!/usr/bin/python3

import sys
from easydict import EasyDict as edict

inFile = sys.argv[1]
outFile = sys.argv[2]

out = open(outFile, "w")
block = edict({"b": []})


def handleBlock(line):
    if line.strip() == "":
        out.write("\016".join(block.b))
        out.write("\n")
        block.b = []
    else:
        cell = line.split(":", 1)[1].strip()
        block.b.append(cell)

with open(inFile) as inF:
    for line in inF:
        handleBlock(line)

out.close()
