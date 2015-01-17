#!/usr/bin/python3

import sys
from easydict import EasyDict as edict

inFile = sys.argv[1]
outFile = sys.argv[2]

out = open(outFile, "w")
block = edict({"b": []})
useNum = True

block.users = {}
block.products = {}


def checkItem(item, dic):
    if item not in dic:
        cell = len(dic)
        dic[item] = cell
    else:
        cell = dic[item]
    return str(cell)


def handleBlock(line):
    if line.strip() == "":
        out.write("\016".join(block.b))
        out.write("\n")
        block.b = []
    else:
        cells = line.split(":", 1)
        if useNum:
            if cells[0] == "product/productId":
                name = checkItem(cells[1], block.products)
            elif cells[0] == "review/userId":
                name = checkItem(cells[1], block.users)
            else:
                name = cells[1].strip()
        else:
            name = cells[1].strip()
        block.b.append(name)

with open(inFile) as inF:
    for line in inF:
        handleBlock(line)

out.close()
